package notifier

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"io"
	"math/big"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/http"
	"net/http/httptest"
	"net/mail"
	"net/textproto"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPlanDelivery(t *testing.T) {
	slackOn := SlackChannel{Enabled: true}
	emailOn := EmailChannel{Enabled: true}
	list := []string{"oncall@example.com"}
	emailWithList := EmailChannel{Enabled: true, ExtraRecipients: list}
	adminBlocksRunStatus := EmailChannel{Enabled: true, Muted: []string{CategoryRunStatus}, ExtraRecipients: list}
	defaults := DefaultEmailPreferences()
	ownerOff := EmailPreferences{Enabled: false}

	cases := []struct {
		name       string
		cfg        ChannelConfig
		category   string
		prefs      EmailPreferences
		wantSlack  bool
		wantOwner  bool
		wantList   []string
		wantStatus string
	}{
		{"nothing configured", ChannelConfig{}, CategoryDataLoss, defaults, false, false, nil, StatusSuppressed},
		{"both on", ChannelConfig{Slack: slackOn, Email: emailOn}, CategoryHealth, defaults, true, true, nil, ""},
		{"slack muted for category", ChannelConfig{Slack: SlackChannel{Enabled: true, Muted: []string{CategoryHealth}}, Email: emailOn}, CategoryHealth, defaults, false, true, nil, ""},
		{"slack mute of another category", ChannelConfig{Slack: SlackChannel{Enabled: true, Muted: []string{CategoryHealth}}}, CategoryDataLoss, defaults, true, false, nil, ""},
		{"owner turned email off", ChannelConfig{Email: emailOn}, CategoryRunStatus, ownerOff, false, false, nil, StatusSkipped},
		{"owner muted the category", ChannelConfig{Email: emailOn}, CategoryRunStatus, EmailPreferences{Enabled: true, Muted: []string{CategoryRunStatus}}, false, false, nil, StatusSkipped},
		{"owner mute ignores Slack", ChannelConfig{Slack: slackOn, Email: emailOn}, CategoryRunStatus, ownerOff, true, false, nil, ""},
		{"other is on by default", ChannelConfig{Slack: slackOn}, CategoryOther, defaults, true, false, nil, ""},

		// Admin email controls.
		{"alert list with owner", ChannelConfig{Email: emailWithList}, CategoryHealth, defaults, false, true, list, ""},
		{"alert list ignores the owner's email switch", ChannelConfig{Email: emailWithList}, CategoryHealth, ownerOff, false, false, list, ""},
		{"alert list ignores the owner's mute", ChannelConfig{Email: emailWithList}, CategoryHealth, EmailPreferences{Enabled: true, Muted: []string{CategoryHealth}}, false, false, list, ""},
		{"admin block stops the owner and the list", ChannelConfig{Email: adminBlocksRunStatus}, CategoryRunStatus, defaults, false, false, nil, StatusSkipped},
		{"admin block leaves Slack alone", ChannelConfig{Slack: slackOn, Email: adminBlocksRunStatus}, CategoryRunStatus, defaults, true, false, nil, ""},
		{"admin block of another category", ChannelConfig{Email: adminBlocksRunStatus}, CategoryDataLoss, defaults, false, true, list, ""},
		{"alert list needs email enabled", ChannelConfig{Slack: slackOn, Email: EmailChannel{ExtraRecipients: list}}, CategoryHealth, defaults, true, false, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planDelivery(tc.cfg, tc.category, tc.prefs)
			if got.slack != tc.wantSlack || got.ownerEmail != tc.wantOwner || !reflect.DeepEqual(got.listEmail, tc.wantList) || got.status != tc.wantStatus {
				t.Errorf("planDelivery = %+v, want slack=%v owner=%v list=%v status=%q", got, tc.wantSlack, tc.wantOwner, tc.wantList, tc.wantStatus)
			}
		})
	}
}

// ── Slack ──────────────────────────────────────────────────────────────────

func TestSendSlackWebhook(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if strings.HasSuffix(r.URL.Path, "/revoked") {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("invalid_token"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	ctx := context.Background()

	err := sendSlackWebhook(ctx, srv.Client(), SlackChannel{Enabled: true, WebhookURL: srv.URL + "/services/ok"}, map[string]interface{}{"text": "hello"})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if gotBody != `{"text":"hello"}` {
		t.Errorf("body = %s", gotBody)
	}

	err = sendSlackWebhook(ctx, srv.Client(), SlackChannel{Enabled: true, WebhookURL: srv.URL + "/services/revoked"}, map[string]interface{}{"text": "x"})
	if err == nil || !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "invalid_token") {
		t.Errorf("want the HTTP status and Slack's reason, got %v", err)
	}

	secretErr := errors.New("could not be decrypted")
	if err := sendSlackWebhook(ctx, srv.Client(), SlackChannel{Enabled: true, SecretErr: secretErr}, nil); !errors.Is(err, secretErr) {
		t.Errorf("want SecretErr, got %v", err)
	}
	if err := sendSlackWebhook(ctx, srv.Client(), SlackChannel{Enabled: true}, nil); err == nil {
		t.Error("an empty webhook URL must be an error, not a silent success")
	}
}

// The webhook URL is the credential. A transport error must not carry it into
// delivery_error (stored on the row, shown in the inbox) or the logs.
func TestSendSlackWebhookErrorDoesNotLeakURL(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: connection refused

	const secretPath = "/services/T0000/B0000/SuperSecretToken"
	err = sendSlackWebhook(context.Background(), &http.Client{Timeout: 2 * time.Second},
		SlackChannel{Enabled: true, WebhookURL: "http://" + addr + secretPath}, map[string]interface{}{"text": "x"})
	if err == nil {
		t.Fatal("expected a connection error")
	}
	if strings.Contains(err.Error(), "SuperSecretToken") || strings.Contains(err.Error(), secretPath) {
		t.Errorf("error leaks the webhook URL: %v", err)
	}
}

// ── Email rendering ────────────────────────────────────────────────────────

func TestBuildEmailHeaderInjection(t *testing.T) {
	from := &mail.Address{Address: "alerts@example.com"}
	to := &mail.Address{Address: "owner@example.com"}
	raw, err := buildEmail(from, to, emailMessage{
		Subject: "[rsync-ai ERROR] orders\r\nBcc: attacker@example.com",
		Body:    "Línea uno\nline two\r\n.\nend",
	}, time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := mail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("not a parseable message: %v\n%s", err, raw)
	}
	if bcc := msg.Header.Get("Bcc"); bcc != "" {
		t.Fatalf("subject injected a Bcc header: %q", bcc)
	}
	subject, err := new(mime.WordDecoder).DecodeHeader(msg.Header.Get("Subject"))
	if err != nil {
		t.Fatal(err)
	}
	if subject != "[rsync-ai ERROR] orders Bcc: attacker@example.com" {
		t.Errorf("subject = %q", subject)
	}
	body, err := io.ReadAll(quotedprintable.NewReader(msg.Body))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "Línea uno\r\nline two\r\n.\r\nend" {
		t.Errorf("body round-trip = %q", body)
	}
	if msg.Header.Get("Date") == "" || msg.Header.Get("MIME-Version") != "1.0" {
		t.Errorf("missing Date or MIME-Version: %v", msg.Header)
	}
}

// ── SMTP, against an in-process server ─────────────────────────────────────

type fakeSMTPOptions struct {
	implicitTLS bool
	offerTLS    bool // advertise STARTTLS
	cert        *tls.Certificate
}

// smtpSession is what the fake server saw from one client.
type smtpSession struct {
	usedTLS  bool
	authLine string
	mailFrom string
	rcptTo   string
	data     string
}

type fakeSMTP struct {
	addr string

	mu sync.Mutex
	smtpSession
	// delivered: every message accepted, in order.
	delivered []smtpSession
}

func (f *fakeSMTP) snapshot() smtpSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.smtpSession
}

func (f *fakeSMTP) all() []smtpSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]smtpSession(nil), f.delivered...)
}

func startFakeSMTP(t *testing.T, opt fakeSMTPOptions) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var tlsCfg *tls.Config
	if opt.cert != nil {
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{*opt.cert}}
	}
	if opt.implicitTLS {
		ln = tls.NewListener(ln, tlsCfg)
	}
	f := &fakeSMTP{addr: ln.Addr().String()}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn, opt, tlsCfg)
		}
	}()
	return f
}

func (f *fakeSMTP) serve(conn net.Conn, opt fakeSMTPOptions, tlsCfg *tls.Config) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	secure := opt.implicitTLS
	tp := textproto.NewConn(conn)
	reply := func(s string) { _ = tp.PrintfLine("%s", s) }
	reply("220 fake ESMTP")
	for {
		line, err := tp.ReadLine()
		if err != nil {
			return
		}
		verb := strings.ToUpper(strings.SplitN(line, " ", 2)[0])
		switch verb {
		case "EHLO", "HELO":
			lines := []string{"250-fake"}
			if opt.offerTLS && !secure {
				lines = append(lines, "250-STARTTLS")
			}
			if secure || !opt.offerTLS {
				lines = append(lines, "250-AUTH PLAIN")
			}
			lines = append(lines, "250 OK")
			for _, l := range lines {
				reply(l)
			}
		case "STARTTLS":
			reply("220 go ahead")
			tlsConn := tls.Server(conn, tlsCfg)
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			conn = tlsConn
			tp = textproto.NewConn(tlsConn)
			secure = true
		case "AUTH":
			f.mu.Lock()
			f.authLine = line
			f.mu.Unlock()
			reply("235 authenticated")
		case "MAIL":
			f.mu.Lock()
			f.mailFrom = line
			f.usedTLS = secure
			f.mu.Unlock()
			reply("250 OK")
		case "RCPT":
			f.mu.Lock()
			f.rcptTo = line
			f.mu.Unlock()
			reply("250 OK")
		case "DATA":
			reply("354 send it")
			b, err := tp.ReadDotBytes()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.data = string(b)
			f.delivered = append(f.delivered, f.smtpSession)
			f.mu.Unlock()
			reply("250 queued")
		case "QUIT":
			reply("221 bye")
			return
		default:
			reply("250 OK")
		}
	}
}

// selfSignedCert returns a cert for 127.0.0.1 and trusts it for the duration of
// the test via smtpRootCAs.
func selfSignedCert(t *testing.T) *tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fake smtp"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	prev := smtpRootCAs
	smtpRootCAs = pool
	t.Cleanup(func() { smtpRootCAs = prev })
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func emailChannelFor(t *testing.T, f *fakeSMTP, mode string) EmailChannel {
	t.Helper()
	host, portStr, _ := net.SplitHostPort(f.addr)
	port, _ := strconv.Atoi(portStr)
	return EmailChannel{Enabled: true, Host: host, Port: port, From: "rsync-ai <alerts@example.com>", TLSMode: mode}
}

func TestSendSMTPPlaintextRelay(t *testing.T) {
	f := startFakeSMTP(t, fakeSMTPOptions{})
	ch := emailChannelFor(t, f, TLSModeNone)
	if err := sendSMTP(context.Background(), ch, "owner@example.com", emailMessage{Subject: "s", Body: "b"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	got := f.snapshot()
	if got.mailFrom != "MAIL FROM:<alerts@example.com>" || !strings.HasPrefix(got.rcptTo, "RCPT TO:<owner@example.com>") {
		t.Errorf("envelope = %q / %q", got.mailFrom, got.rcptTo)
	}
	if got.authLine != "" {
		t.Errorf("authenticated without credentials: %q", got.authLine)
	}
	if !strings.Contains(got.data, "Subject: s") {
		t.Errorf("message not delivered: %q", got.data)
	}
}

// "starttls" is a promise that the password and the alert do not cross the
// network in the clear. A server that does not offer STARTTLS must fail the
// send, not quietly downgrade it.
func TestSendSMTPStartTLSRequiredIsNotDowngraded(t *testing.T) {
	f := startFakeSMTP(t, fakeSMTPOptions{offerTLS: false})
	ch := emailChannelFor(t, f, TLSModeStartTLS)
	ch.Username, ch.Password = "user", "hunter2"
	err := sendSMTP(context.Background(), ch, "owner@example.com", emailMessage{Subject: "s", Body: "b"})
	if err == nil || !strings.Contains(err.Error(), "does not offer STARTTLS") {
		t.Fatalf("want a STARTTLS error, got %v", err)
	}
	if got := f.snapshot(); got.authLine != "" || got.mailFrom != "" {
		t.Errorf("sent credentials or mail over plaintext: auth=%q mail=%q", got.authLine, got.mailFrom)
	}
}

func TestSendSMTPStartTLSWithAuth(t *testing.T) {
	cert := selfSignedCert(t)
	f := startFakeSMTP(t, fakeSMTPOptions{offerTLS: true, cert: cert})
	ch := emailChannelFor(t, f, TLSModeStartTLS)
	ch.Username, ch.Password = "user", "hunter2"
	if err := sendSMTP(context.Background(), ch, "owner@example.com", emailMessage{Subject: "s", Body: "b"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	got := f.snapshot()
	if !got.usedTLS {
		t.Error("mail was sent before STARTTLS")
	}
	want := "AUTH PLAIN " + base64.StdEncoding.EncodeToString([]byte("\x00user\x00hunter2"))
	if got.authLine != want {
		t.Errorf("auth line = %q, want %q", got.authLine, want)
	}
}

func TestSendSMTPImplicitTLS(t *testing.T) {
	cert := selfSignedCert(t)
	f := startFakeSMTP(t, fakeSMTPOptions{implicitTLS: true, cert: cert})
	ch := emailChannelFor(t, f, TLSModeTLS)
	if err := sendSMTP(context.Background(), ch, "owner@example.com", emailMessage{Subject: "s", Body: "b"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := f.snapshot(); !got.usedTLS || got.data == "" {
		t.Errorf("implicit TLS send did not complete: %+v", got)
	}
}

// An untrusted certificate must fail, not be accepted.
func TestSendSMTPRejectsUntrustedCertificate(t *testing.T) {
	cert := selfSignedCert(t)
	smtpRootCAs = x509.NewCertPool() // trust nothing (restored by selfSignedCert's cleanup)
	f := startFakeSMTP(t, fakeSMTPOptions{implicitTLS: true, cert: cert})
	err := sendSMTP(context.Background(), emailChannelFor(t, f, TLSModeTLS), "owner@example.com", emailMessage{Subject: "s", Body: "b"})
	if err == nil {
		t.Fatal("sent over TLS to an untrusted certificate")
	}
}

func TestSendSMTPValidation(t *testing.T) {
	ctx := context.Background()
	secretErr := errors.New("could not be decrypted")
	cases := []struct {
		name string
		ch   EmailChannel
		to   string
	}{
		{"secret error", EmailChannel{Enabled: true, Host: "h", From: "a@b.c", SecretErr: secretErr}, "o@e.com"},
		{"no host", EmailChannel{Enabled: true, From: "a@b.c"}, "o@e.com"},
		{"bad from", EmailChannel{Enabled: true, Host: "h", From: "not an address"}, "o@e.com"},
		{"bad recipient", EmailChannel{Enabled: true, Host: "h", From: "a@b.c"}, "nobody"},
		{"unknown mode", EmailChannel{Enabled: true, Host: "127.0.0.1", Port: 1, From: "a@b.c", TLSMode: "ssl3"}, "o@e.com"},
	}
	for _, tc := range cases {
		if err := sendSMTP(ctx, tc.ch, tc.to, emailMessage{}); err == nil {
			t.Errorf("%s: want an error", tc.name)
		}
	}
	if err := sendSMTP(ctx, cases[0].ch, "o@e.com", emailMessage{}); !errors.Is(err, secretErr) {
		t.Errorf("want SecretErr surfaced, got %v", err)
	}
}

// The test message's link must open the page where channels are managed, not
// the admin landing page. Slack builds the same URL but its client refuses
// loopback, so the email path pins it.
func TestSendTestNotificationLinksToNotificationSettings(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", testEncryptionKey)
	clearNotifierEnv(t)
	t.Setenv("APP_BASE_URL", "https://rsync.test/")
	f := startFakeSMTP(t, fakeSMTPOptions{})
	ch := emailChannelFor(t, f, TLSModeNone)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	mock.ExpectQuery(`FROM notification_channel_settings`).WillReturnRows(
		sqlmock.NewRows(channelColumns).AddRow(
			false, "", "",
			true, ch.Host, ch.Port, "", "",
			ch.From, TLSModeNone, "", "", time.Now()))

	if err := SendTestNotification(context.Background(), mockDB, "email", "admin@example.com"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := f.snapshot().data; !strings.Contains(got, "https://rsync.test/admin/notifications") {
		t.Errorf("test email does not link to /admin/notifications:\n%s", got)
	}
}
