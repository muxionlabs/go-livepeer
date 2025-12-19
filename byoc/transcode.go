package byoc

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/clog"
)

// Trimmed down version of authWebhookResponse with AI parameters
//
//	See type defined in server/mediaserver.go for full version used in HandlePush transcoding requests
type authWebhookResponseWithAI struct {
	ManifestID string          `json:"manifestID"`
	AIParams   json.RawMessage `json:"aiParams"`
}

func (bsg *BYOCGatewayServer) TranscodeWithAIStream() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transcodeConfigurationHeader := r.Header.Get("Livepeer-Transcode-Configuration")

		var transcodeConfiguration authWebhookResponseWithAI
		if err := json.Unmarshal([]byte(transcodeConfigurationHeader), &transcodeConfiguration); err != nil {
			httpErr := fmt.Sprintf(`failed to parse transcode config header: %q`, err)
			glog.Error(httpErr)
			http.Error(w, httpErr, http.StatusBadRequest)
			return
		}

		// Fallback
		if len(transcodeConfiguration.AIParams) == 0 {
			bsg.transcodePush(w, r)
			return
		}

		//clone the request so can push body to HandlePush and BYOC StartStream
		clonedReq, aiStreamCloneErr := cloneSegmentRequest(r)
		if aiStreamCloneErr != nil {
			clog.Errorf(r.Context(), "Failed to clone request: %s", aiStreamCloneErr)
			return
		}
		body, err := io.ReadAll(clonedReq.Body)
		if err != nil {
			clog.Errorf(r.Context(), "Failed to read cloned request body: %s", err)
			return
		}
		defer clonedReq.Body.Close()

		//send the request on to HandlePush and BYOC StartStream concurrently
		var (
			handlePushResp  = NewCaptureResponseWriter()
			startStreamResp = NewCaptureResponseWriter()
			wg              sync.WaitGroup
		)

		wg.Add(2)

		// HandlePush
		go func() {
			defer wg.Done()

			req := r.Clone(r.Context())

			// Replace /process/transcode with /live
			req.URL.Path = strings.Replace(
				req.URL.Path,
				"/process/transcode",
				"/live",
				1,
			)

			bsg.transcodePush(handlePushResp, req)
		}()

		// Start AI Stream if does not exist, otherwise push segment to stream
		_, aiStreamRunning := bsg.StreamPipelines[transcodeConfiguration.ManifestID]
		if aiStreamRunning {
			go func() {
				defer wg.Done()
				glog.Infof("Pushing segment to existing AI stream for manifestID=%s", transcodeConfiguration.ManifestID)
				bsg.PushSegment(transcodeConfiguration.ManifestID, body)
			}()
		} else {
			go func() {
				defer wg.Done()

				req, err := http.NewRequestWithContext(
					r.Context(),
					http.MethodPost,
					"http://localhost:5937/process/stream/start",
					bytes.NewReader(transcodeConfiguration.AIParams),
				)
				if err != nil {
					clog.Errorf(r.Context(), "StartStream request error: %s", err)
					return
				}

				req.Header.Set(
					"Livepeer",
					base64.StdEncoding.EncodeToString(transcodeConfiguration.AIParams),
				)
				req.Header.Set("Content-Type", "application/json")

				bsg.StartStream().ServeHTTP(startStreamResp, req)

				glog.Infof("Pushing segment to NEW AI stream for manifestID=%s", transcodeConfiguration.ManifestID)
				bsg.PushSegment(transcodeConfiguration.ManifestID, body)
			}()
		}

		wg.Wait()

		// ---- Merge responses ----

		// Copy HandlePush headers
		for k, v := range handlePushResp.Header() {
			w.Header()[k] = v
		}

		if !aiStreamRunning {
			// Add AI stream URLs if StartStream succeeded
			if startStreamResp.StatusCode() == http.StatusOK && startStreamResp.Buffer.Len() > 0 {
				w.Header().Set(
					"X-AI-Stream-Urls",
					base64.StdEncoding.EncodeToString(startStreamResp.Buffer.Bytes()),
				)
			}
		} else {
			// TODO: add some reporting via header on AI stream
		}

		// Final write
		w.WriteHeader(handlePushResp.StatusCode())
		_, _ = w.Write(handlePushResp.Buffer.Bytes())
	})
}

// HandlePushV2 processes request for HTTP ingest with AI parameters
//
//	Sends the request to the HandlePush route and sends the request to the
//	BYOC /ai/stream/start endpoint if AI parameters are present
//
// CaptureResponseWriter is a custom response writer that captures the response body and headers
type CaptureResponseWriter struct {
	header     http.Header
	Buffer     *bytes.Buffer
	statusCode int
}

func NewCaptureResponseWriter() *CaptureResponseWriter {
	return &CaptureResponseWriter{
		header:     make(http.Header),
		Buffer:     &bytes.Buffer{},
		statusCode: http.StatusOK,
	}
}

func (c *CaptureResponseWriter) Header() http.Header {
	return c.header
}

func (c *CaptureResponseWriter) WriteHeader(statusCode int) {
	c.statusCode = statusCode
}

func (c *CaptureResponseWriter) Write(p []byte) (int, error) {
	return c.Buffer.Write(p)
}

func (c *CaptureResponseWriter) StatusCode() int {
	return c.statusCode
}

func cloneSegmentRequest(r *http.Request) (*http.Request, error) {
	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		// Restore original body
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	clone := r.Clone(r.Context())
	clone.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	return clone, nil
}
