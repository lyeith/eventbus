//go:build performance

package ses

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/stretchr/testify/require"
)

// PDF setup, independent attachment verification and capture are outside timing.
// The measured scope is native SES v2 raw MIME validation: transport-base64
// decoding, line limits, headers, attachment policy and recursive MIME decoding.
func TestPerformanceRound2SESRawMIME(t *testing.T) {
	for _, payloadMiB := range []int{1, 16} {
		t.Run(fmt.Sprintf("pdf_payload_%dMiB", payloadMiB), func(t *testing.T) {
			attachment := sesRound2PDF(payloadMiB << 20)
			checksum := sha256.Sum256(attachment)
			encodedAttachment := base64.StdEncoding.EncodeToString(attachment)
			var raw strings.Builder
			raw.WriteString("From: sender@example.com\r\nTo: recipient@example.com\r\nSubject: Audit PDF\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=round2-owned\r\n\r\n")
			raw.WriteString("--round2-owned\r\nContent-Type: application/pdf; name=audit.pdf\r\nContent-Disposition: attachment; filename=audit.pdf\r\nContent-Transfer-Encoding: base64\r\n\r\n")
			for offset := 0; offset < len(encodedAttachment); offset += 76 {
				raw.WriteString(encodedAttachment[offset:min(offset+76, len(encodedAttachment))])
				raw.WriteString("\r\n")
			}
			raw.WriteString("--round2-owned--\r\n")
			encodedRequest := base64.StdEncoding.EncodeToString([]byte(raw.String()))
			attachmentBytes := len(attachment)
			rawBytes := raw.Len()
			sesRound2VerifyAttachment(t, encodedRequest, attachmentBytes, checksum)
			// Release large setup-only buffers before the first indexed sample;
			// no forced GC or setup work occurs inside a measured interval.
			attachment = nil
			encodedAttachment = ""
			raw.Reset()
			runtime.GC()

			var wall, allocBytes, allocCount []float64
			for range 5 {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				email, apiErr := sesValidateRaw("v2", encodedRequest, sesV2MaxMessageBytes)
				wall = append(wall, float64(time.Since(started))/float64(time.Millisecond))
				runtime.ReadMemStats(&after)
				allocBytes = append(allocBytes, float64(after.TotalAlloc-before.TotalAlloc))
				allocCount = append(allocCount, float64(after.Mallocs-before.Mallocs))
				require.Nil(t, apiErr)
				require.Equal(t, "sender@example.com", email["from"])
				require.Equal(t, "Audit PDF", email["subject"])
				require.Equal(t, []any{"recipient@example.com"}, sesObject(email["destination"])["ToAddresses"])
				require.NotContains(t, email, "raw", "normalized metadata must not duplicate attachment bytes")
			}
			t.Logf("PERFORMANCE_SES_RAW attachment_bytes=%d decoded_mime_bytes=%d encoded_request_bytes=%d", attachmentBytes, rawBytes, len(encodedRequest))
			testperf.Report(t, t.Name(), "native_validation_ms", wall)
			testperf.Report(t, t.Name(), "allocated_bytes", allocBytes)
			testperf.Report(t, t.Name(), "allocations", allocCount)
		})
	}
}

// One valid PDF page carries a large comment in its content stream. Native MIME
// folding happens outside these bytes and must preserve the exact PDF checksum.
func sesRound2PDF(commentBytes int) []byte {
	var pdf bytes.Buffer
	pdf.WriteString("%PDF-1.4\n")
	offsets := []int{0}
	writeObject := func(number int, value string) {
		offsets = append(offsets, pdf.Len())
		fmt.Fprintf(&pdf, "%d 0 obj\n%s\nendobj\n", number, value)
	}
	writeObject(1, "<< /Type /Catalog /Pages 2 0 R >>")
	writeObject(2, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
	writeObject(3, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>")
	offsets = append(offsets, pdf.Len())
	fmt.Fprintf(&pdf, "4 0 obj\n<< /Length %d >>\nstream\n%%", commentBytes+2)
	pdf.Write(bytes.Repeat([]byte{'x'}, commentBytes))
	pdf.WriteString("\nendstream\nendobj\n")
	xref := pdf.Len()
	pdf.WriteString("xref\n0 5\n0000000000 65535 f \n")
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&pdf, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&pdf, "trailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", xref)
	return pdf.Bytes()
}

func sesRound2VerifyAttachment(t *testing.T, encoded string, expectedBytes int, checksum [32]byte) {
	t.Helper()
	message, err := mail.ReadMessage(base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded)))
	require.NoError(t, err)
	kind, parameters, err := mime.ParseMediaType(message.Header.Get("Content-Type"))
	require.NoError(t, err)
	require.Equal(t, "multipart/mixed", kind)
	reader := multipart.NewReader(message.Body, parameters["boundary"])
	part, err := reader.NextRawPart()
	require.NoError(t, err)
	require.Equal(t, "audit.pdf", part.FileName())
	require.Equal(t, "application/pdf; name=audit.pdf", part.Header.Get("Content-Type"))
	digest := sha256.New()
	count, err := io.Copy(digest, base64.NewDecoder(base64.StdEncoding, part))
	require.NoError(t, err)
	require.Equal(t, int64(expectedBytes), count)
	require.Equal(t, checksum[:], digest.Sum(nil))
	require.NoError(t, part.Close())
	_, err = reader.NextRawPart()
	require.ErrorIs(t, err, io.EOF)
}
