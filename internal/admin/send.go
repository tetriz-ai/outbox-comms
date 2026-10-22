// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/mail"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tetriz-ai/outbox-comms/config"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/attachments"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/server"
)

const (
	sendModeDryRun  = "dry-run"
	sendModePreview = "preview"
	sendModeSend    = "send"

	previewKeyPrefix = "adhoc-preview:"
	sendKeyPrefix    = "adhoc-send:"
	previewLabel     = "adhoc-preview"
	maxEntries       = 1000
	previewTTL       = 24 * time.Hour
	submitTimeout    = 30 * time.Second

	// maxAttachmentBytes is the decoded-file ceiling. The senders cap the
	// assembled message at 10 MiB; base64 (about 4/3) and line wrapping make a
	// 6 MiB file about 8.2 MiB, which leaves room for the body and headers.
	maxAttachmentBytes = 6 * 1024 * 1024

	noAttachmentFP = "none"
)

// submitter is the SubmitComms RPC the command uses; the generated
// CommsServiceClient satisfies it.
type submitter interface {
	SubmitComms(ctx context.Context, req *pb.SubmitCommsRequest, opts ...grpc.CallOption) (*pb.SubmitCommsResponse, error)
}

type recipient struct {
	email string
	name  string
}

// sendCmd holds one `admin send` run.
type sendCmd struct {
	e           *env
	org         string
	usecase     string
	contentPath string
	content     *pb.MessageContent
	recipients  []recipient
	reviewers   []string
	mode        string
	previewID   string
	attachPath  string
	attachName  string

	d       *daos
	comms   submitter
	store   attachments.Store
	now     func() time.Time
	newUUID func() string
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

// send is the maker-checker flow for operator-written content. dry-run (the
// default) prints the audience and changes nothing. preview mails the reviewers
// a copy marked as a preview and prints its request id. send needs that id: it
// refuses unless the preview is for the same org, usecase, content, audience and
// attachment, is under 24 hours old, and reached at least one reviewer. It then
// submits the audience in chunks of 1000 under idempotency keys derived from the
// preview id, so a re-run sends nothing twice.
func (e *env) send(ctx context.Context, args []string) error {
	fs := e.flags("send", "send --org UUID --usecase KEY --content FILE --recipients FILE [--mode dry-run|preview|send] [--reviewer EMAIL]... [--preview-id UUID] [--attachment FILE [--attachment-name NAME]] [--addr HOST:PORT]")
	c := &sendCmd{e: e, now: time.Now, newUUID: uuid.NewString}
	fs.StringVar(&c.org, "org", "", "organization the send belongs to (required)")
	fs.StringVar(&c.usecase, "usecase", "", "registered usecase key (required)")
	fs.StringVar(&c.contentPath, "content", "", "MessageContent JSON file (required)")
	recipientsPath := fs.String("recipients", "", "file with one recipient per line: email, or email,Name (required)")
	var reviewers multiFlag
	fs.Var(&reviewers, "reviewer", "reviewer email for --mode preview (repeatable)")
	fs.StringVar(&c.mode, "mode", sendModeDryRun, "dry-run, preview, or send")
	fs.StringVar(&c.previewID, "preview-id", "", "request id printed by --mode preview (required for --mode send)")
	fs.StringVar(&c.attachPath, "attachment", "", "file to attach")
	fs.StringVar(&c.attachName, "attachment-name", "", "filename recipients see (default: the file's name)")
	addr := fs.String("addr", "localhost:9090", "the service's gRPC address")
	if err := e.parse(fs, args); err != nil {
		return err
	}
	if err := c.validate(*recipientsPath, reviewers); err != nil {
		return err
	}
	if err := e.ready(); err != nil {
		return err
	}

	d, closeDB, err := e.openDAOs()
	if err != nil {
		return err
	}
	defer closeDB()
	c.d = d
	if c.mode != sendModeDryRun {
		conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial %s: %w", *addr, err)
		}
		defer func() { _ = conn.Close() }()
		c.comms = pb.NewCommsServiceClient(conn)
		c.store, err = attachmentStore(ctx, e.cfg)
		if err != nil {
			return err
		}
	}
	return c.run(ctx)
}

func (c *sendCmd) validate(recipientsPath string, reviewers []string) error {
	// The org is stored and compared in the lowercase hyphenated form the
	// service stores, so any case the operator types names the same preview.
	id, err := uuid.Parse(c.org)
	if err != nil || len(c.org) != 36 {
		return usageErr("--org must be a UUID in the 36-character form, got %q", c.org)
	}
	c.org = id.String()
	if c.usecase == "" {
		return usageErr("--usecase is required")
	}
	if c.contentPath == "" {
		return usageErr("--content is required")
	}
	content, err := loadContent(c.contentPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(content.GetBase().GetSubject()) == "" {
		return usageErr("content %s is missing base.subject", c.contentPath)
	}
	c.content = content
	if recipientsPath == "" {
		return usageErr("--recipients is required")
	}
	c.recipients, err = readRecipients(recipientsPath)
	if err != nil {
		return err
	}
	c.reviewers = normalizeEmails(reviewers)
	switch c.mode {
	case sendModeDryRun, sendModePreview, sendModeSend:
	default:
		return usageErr("--mode must be dry-run, preview or send, got %q", c.mode)
	}
	if c.mode == sendModePreview && len(c.reviewers) == 0 {
		return usageErr("--mode preview needs at least one --reviewer")
	}
	if c.mode == sendModeSend {
		if _, err := uuid.Parse(c.previewID); err != nil {
			return usageErr("--mode send needs --preview-id, the request id --mode preview printed")
		}
	}
	if c.attachPath == "" && c.attachName != "" {
		return usageErr("--attachment-name needs --attachment")
	}
	if c.attachPath != "" {
		if _, err := readAttachment(c.attachPath); err != nil {
			return usageErr("--attachment: %v", err)
		}
		if c.attachName == "" {
			c.attachName = filepath.Base(c.attachPath)
		}
	}
	return nil
}

func (c *sendCmd) run(ctx context.Context) error {
	out := c.e.stdout
	fingerprint := audienceFingerprint(audienceLines(c.recipients))
	switch c.mode {
	case sendModeDryRun:
		fmt.Fprintf(out, "audience: %d recipient(s) in %d chunk(s), fingerprint %s\n", len(c.recipients), chunkCount(len(c.recipients)), fingerprint)
		for _, r := range c.recipients {
			fmt.Fprintf(out, "  %s %s\n", maskEmail(r.email), r.name)
		}
		if c.attachPath != "" {
			data, err := readAttachment(c.attachPath)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "attachment: %s, %d bytes, sha256 %s (uploaded on --mode preview)\n", c.attachName, len(data), sha256Hex(data))
		}
		fmt.Fprintln(out, "dry run: nothing submitted; next, --mode preview with --reviewer")
		return nil
	case sendModePreview:
		return c.preview(ctx, fingerprint)
	default:
		return c.sendAll(ctx, fingerprint)
	}
}

func (c *sendCmd) preview(ctx context.Context, fingerprint string) error {
	handle := previewKeyPrefix + c.org + ":" + c.usecase + ":" + c.newUUID()
	contentFP, err := contentFingerprint(c.content)
	if err != nil {
		return err
	}
	attachKey, attachName, attachFP, err := c.uploadAttachment(ctx)
	if err != nil {
		return err
	}
	label := previewAnalytics(c.usecase, len(c.recipients), fingerprint, contentFP, attachFP)
	body := withPreviewBanner(c.content, len(c.recipients))
	entries := make([]*pb.CommsEntry, 0, len(c.reviewers))
	for _, r := range c.reviewers {
		entries = append(entries, &pb.CommsEntry{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: r, Content: body})
	}
	row, err := c.submit(ctx, handle, label, body, entries, attachKey, attachName)
	if err != nil {
		return fmt.Errorf("submit preview: %w", err)
	}
	fmt.Fprintf(c.e.stdout, "preview submitted to %d reviewer(s) for an audience of %d\n", len(c.reviewers), len(c.recipients))
	fmt.Fprintf(c.e.stdout, "preview id: %s\n", row.ID)
	fmt.Fprintln(c.e.stdout, "after a reviewer receives it, run again with --mode send --preview-id", row.ID)
	return nil
}

func (c *sendCmd) sendAll(ctx context.Context, fingerprint string) error {
	preview, err := c.d.request.GetByID(ctx, c.org, c.previewID)
	if err != nil {
		return fmt.Errorf("load preview %s: %w", c.previewID, err)
	}
	attachKey, attachName, attachFP, err := c.sendAttachment(preview)
	if err != nil {
		return err
	}
	chunks := chunkRecipients(c.recipients, maxEntries)
	// A send that already submitted a chunk under this preview is resumed even
	// past the preview's age limit: its chunk keys are fixed by the preview id,
	// so finishing it sends nothing twice, while a fresh preview would mint new
	// keys and send the submitted chunks again. Only a fresh send needs a fresh
	// preview.
	resuming := false
	for i := range chunks {
		existing, err := c.d.request.GetByHandle(ctx, c.org, sendIdempotencyKey(c.previewID, i, len(chunks)))
		if err != nil && !errors.Is(err, dao.ErrNotFound) {
			return fmt.Errorf("look for chunks already submitted: %w", err)
		}
		if err == nil && existing != nil {
			resuming = true
			break
		}
	}
	if err := c.checkPreview(preview, fingerprint, attachFP, resuming); err != nil {
		return err
	}
	if err := c.checkPreviewDelivered(ctx); err != nil {
		return err
	}
	for i, chunk := range chunks {
		handle := sendIdempotencyKey(c.previewID, i, len(chunks))
		if existing, err := c.d.request.GetByHandle(ctx, c.org, handle); err == nil && existing != nil {
			fmt.Fprintf(c.e.stdout, "chunk %d/%d already submitted as %s; skipping\n", i+1, len(chunks), existing.ID)
			continue
		}
		entries := make([]*pb.CommsEntry, 0, len(chunk))
		for _, r := range chunk {
			entries = append(entries, &pb.CommsEntry{
				Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS,
				Recipient: r.email, Content: contentWithGreeting(c.content, r.name),
			})
		}
		row, err := c.submit(ctx, handle, sendKeyPrefix+c.previewID, c.content, entries, attachKey, attachName)
		if err != nil {
			return fmt.Errorf("submit chunk %d: %w", i+1, err)
		}
		fmt.Fprintf(c.e.stdout, "chunk %d/%d submitted as %s (%d recipients)\n", i+1, len(chunks), row.ID, len(chunk))
	}
	return nil
}

// checkPreview refuses a send that does not match its preview. The preview was
// loaded scoped to c.org, so it always belongs to this organization. resuming
// lifts only the age limit (see sendAll).
func (c *sendCmd) checkPreview(preview *model.CommsRequest, fingerprint, attachFP string, resuming bool) error {
	if preview.Usecase != c.usecase {
		return fmt.Errorf("preview %s is usecase %s, not %s", c.previewID, preview.Usecase, c.usecase)
	}
	if !strings.HasPrefix(preview.IdempotencyKey, previewKeyPrefix) {
		return fmt.Errorf("request %s is not a preview", c.previewID)
	}
	if !resuming && c.now().Sub(preview.CreatedAt) > previewTTL {
		return fmt.Errorf("preview %s is older than %s; run --mode preview again", c.previewID, previewTTL)
	}
	contentFP, err := contentFingerprint(c.content)
	if err != nil {
		return err
	}
	want := previewAnalytics(c.usecase, len(c.recipients), fingerprint, contentFP, attachFP)
	got := ""
	if preview.AnalyticsID != nil {
		got = *preview.AnalyticsID
	}
	if got != want {
		return fmt.Errorf("the audience, content or attachment changed since the preview; run --mode preview again")
	}
	// The label is a field any producer can set, so the content is also checked
	// against what the preview actually stored and the reviewers received.
	if !proto.Equal(preview.Content, withPreviewBanner(c.content, len(c.recipients))) {
		return fmt.Errorf("the content differs from what preview %s sent the reviewers; run --mode preview again", c.previewID)
	}
	return nil
}

func (c *sendCmd) checkPreviewDelivered(ctx context.Context) error {
	msgs, err := c.d.message.ListByRequest(ctx, c.org, c.previewID)
	if err != nil {
		return fmt.Errorf("load preview messages: %w", err)
	}
	if len(msgs) == 0 {
		return fmt.Errorf("preview %s has no messages yet; wait for the fan-out", c.previewID)
	}
	for _, m := range msgs {
		switch m.Status {
		case pb.MessageStatus_MESSAGE_STATUS_SENT, pb.MessageStatus_MESSAGE_STATUS_DELIVERED:
			return nil
		case pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED:
			// The provider accepted it and only the delivery event is missing. An
			// uncertain send may never have left, so it does not count.
			if m.UnconfirmedReason != nil && *m.UnconfirmedReason == "no_delivery_feedback" {
				return nil
			}
		default:
		}
	}
	return fmt.Errorf("preview %s has not reached any reviewer yet", c.previewID)
}

func (c *sendCmd) submit(ctx context.Context, handle, label string, content *pb.MessageContent, entries []*pb.CommsEntry, attachKey, attachName string) (*model.CommsRequest, error) {
	fields := map[string]any{}
	if attachKey != "" {
		fields[sender.AttachmentKeyParam] = attachKey
		fields[sender.AttachmentFilenameParam] = attachName
	}
	params, err := structpb.NewStruct(fields)
	if err != nil {
		return nil, fmt.Errorf("build params: %w", err)
	}
	sctx, cancel := context.WithTimeout(ctx, submitTimeout)
	defer cancel()
	resp, err := c.comms.SubmitComms(sctx, &pb.SubmitCommsRequest{
		OrganizationId: c.org, IdempotencyKey: handle, Usecase: c.usecase,
		Params: params, Content: content, AnalyticsId: label, Entries: entries,
	})
	if err != nil {
		return nil, fmt.Errorf("SubmitComms: %w", err)
	}
	row, err := c.d.request.GetByHandle(ctx, c.org, resp.GetHandle())
	if err != nil {
		return nil, fmt.Errorf("load submitted request %s: %w", resp.GetHandle(), err)
	}
	return row, nil
}

func (c *sendCmd) uploadAttachment(ctx context.Context) (key, name, fp string, err error) {
	if c.attachPath == "" {
		return "", "", noAttachmentFP, nil
	}
	data, err := readAttachment(c.attachPath)
	if err != nil {
		return "", "", "", err
	}
	key = fmt.Sprintf("adhoc/%s/%s", c.newUUID(), sanitizeFilename(c.attachName))
	if err := c.store.Put(ctx, c.org, key, data); err != nil {
		return "", "", "", fmt.Errorf("store attachment: %w", err)
	}
	return key, c.attachName, sha256Hex(data), nil
}

// sendAttachment reuses the preview's stored object, so the send attaches the
// exact bytes the reviewers saw. A local --attachment must still match them.
func (c *sendCmd) sendAttachment(preview *model.CommsRequest) (key, name, fp string, err error) {
	pKey := structString(preview.Params, sender.AttachmentKeyParam)
	pName := structString(preview.Params, sender.AttachmentFilenameParam)
	if c.attachPath == "" {
		if pKey == "" {
			return "", "", noAttachmentFP, nil
		}
		return pKey, pName, previewAttachFingerprint(preview), nil
	}
	if pKey == "" {
		return "", "", "", fmt.Errorf("the preview had no attachment; run --mode preview with --attachment")
	}
	data, err := readAttachment(c.attachPath)
	if err != nil {
		return "", "", "", err
	}
	return pKey, pName, sha256Hex(data), nil
}

// attachmentStore builds the configured store, as the service does, so a file
// the command stores is one the service can read back.
func attachmentStore(ctx context.Context, cfg config.Config) (attachments.Store, error) {
	if cfg.Attachments != "s3" {
		return attachments.Dir{Root: cfg.AttachmentsDir}, nil
	}
	ac, err := server.LoadAWSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return attachments.S3{Client: s3.NewFromConfig(ac), Bucket: cfg.S3Bucket, Prefix: cfg.S3Prefix}, nil
}

func readRecipients(path string) ([]recipient, error) {
	f, err := os.Open(path) //nolint:gosec // an operator-supplied path is the point of the flag
	if err != nil {
		return nil, usageErr("--recipients: %v", err)
	}
	defer func() { _ = f.Close() }()
	seen := map[string]bool{}
	var out []recipient
	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		addr, name, _ := strings.Cut(text, ",")
		addr = strings.ToLower(strings.TrimSpace(addr))
		if a, err := mail.ParseAddress(addr); err != nil || a.Address != addr {
			return nil, usageErr("--recipients line %d: %q is not an email address", line, addr)
		}
		if seen[addr] {
			continue
		}
		seen[addr] = true
		out = append(out, recipient{email: addr, name: strings.TrimSpace(name)})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read recipients: %w", err)
	}
	if len(out) == 0 {
		return nil, usageErr("--recipients %s lists no addresses", path)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].email < out[j].email })
	return out, nil
}

func normalizeEmails(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, raw := range in {
		e := strings.ToLower(strings.TrimSpace(raw))
		if e == "" || seen[e] {
			continue
		}
		seen[e] = true
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

// audienceLines is one line per recipient, address and name, so the preview's
// fingerprint changes when either does.
func audienceLines(rs []recipient) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.email+"\x00"+r.name)
	}
	return out
}

func audienceFingerprint(emails []string) string {
	sorted := append([]string(nil), emails...)
	sort.Strings(sorted)
	return sha256Hex([]byte(strings.Join(sorted, "\n")))
}

func previewAnalytics(usecase string, count int, audienceFP, contentFP, attachFP string) string {
	if attachFP == "" {
		attachFP = noAttachmentFP
	}
	return fmt.Sprintf("%s:%s:%d:%s:%s:%s", previewLabel, usecase, count, audienceFP, contentFP, attachFP)
}

func previewAttachFingerprint(preview *model.CommsRequest) string {
	if preview.AnalyticsID == nil {
		return noAttachmentFP
	}
	parts := strings.Split(*preview.AnalyticsID, ":")
	if len(parts) >= 6 {
		return parts[len(parts)-1]
	}
	return noAttachmentFP
}

func contentFingerprint(content *pb.MessageContent) (string, error) {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("fingerprint content: %w", err)
	}
	return sha256Hex(raw), nil
}

// withPreviewBanner returns a copy of content whose base body and email
// override (the two bodies an email can render) lead with a paragraph marking
// the mail as a preview and naming the audience size, under a [PREVIEW] subject.
func withPreviewBanner(content *pb.MessageContent, intendedCount int) *pb.MessageContent {
	out := proto.Clone(content).(*pb.MessageContent)
	banner := &pb.ContentBlock{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{
		Text: fmt.Sprintf("PREVIEW: this has not been sent to its audience. Intended audience: %d recipient(s).", intendedCount),
	}}}
	for _, rc := range []*pb.RenderableContent{out.GetBase(), out.GetEmail()} {
		if rc == nil {
			continue
		}
		rc.Blocks = append([]*pb.ContentBlock{banner}, rc.GetBlocks()...)
		if rc.GetSubject() != "" && !strings.HasPrefix(rc.GetSubject(), "[PREVIEW] ") {
			rc.Subject = "[PREVIEW] " + rc.GetSubject()
		}
	}
	return out
}

// contentWithGreeting returns a copy of content whose base and email-override
// greetings name the recipient.
func contentWithGreeting(content *pb.MessageContent, name string) *pb.MessageContent {
	if name == "" {
		return content
	}
	out := proto.Clone(content).(*pb.MessageContent)
	for _, rc := range []*pb.RenderableContent{out.GetBase(), out.GetEmail()} {
		if rc != nil {
			rc.GreetingName = name
		}
	}
	return out
}

func chunkRecipients(in []recipient, size int) [][]recipient {
	var out [][]recipient
	for i := 0; i < len(in); i += size {
		end := i + size
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}

func chunkCount(n int) int { return (n + maxEntries - 1) / maxEntries }

func sendIdempotencyKey(previewID string, chunk, chunks int) string {
	if chunks <= 1 {
		return sendKeyPrefix + previewID
	}
	return fmt.Sprintf("%s%s:%d", sendKeyPrefix, previewID, chunk)
}

func readAttachment(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%q is a directory", path)
	}
	data, err := os.ReadFile(path) //nolint:gosec // an operator-supplied path is the point of the flag
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%q is empty", path)
	}
	if len(data) > maxAttachmentBytes {
		return nil, fmt.Errorf("%q is %d bytes, over the %d cap", path, len(data), maxAttachmentBytes)
	}
	return data, nil
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// sanitizeFilename keeps letters, digits, dot, dash and underscore, and caps the
// name at 200 bytes on a rune boundary, keeping the extension.
func sanitizeFilename(name string) string {
	base := filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	var b strings.Builder
	for _, r := range base {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	out := strings.Trim(b.String(), ".")
	if out == "" {
		return "attachment"
	}
	const maxBytes = 200
	if len(out) <= maxBytes {
		return out
	}
	ext := filepath.Ext(out)
	if len(ext) >= maxBytes {
		return truncateBytes(out, maxBytes)
	}
	return truncateBytes(strings.TrimSuffix(out, ext), maxBytes-len(ext)) + ext
}

func truncateBytes(s string, limit int) string {
	for len(s) > limit {
		_, size := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-size]
	}
	return s
}

func structString(s *structpb.Struct, key string) string {
	if s == nil {
		return ""
	}
	return s.GetFields()[key].GetStringValue()
}
