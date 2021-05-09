package router

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func prepareRequest(t *testing.T, method string, url string, body io.Reader) (*assert.Assertions, *httptest.ResponseRecorder, *http.Request) {
	assert := assert.New(t)

	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest(method, url, body)
	assert.NoError(err)

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	recorder := httptest.NewRecorder()
	return assert, recorder, req
}

func TestIndexServerhandler_POST_BadRequest(t *testing.T) {
	// Arrange
	assert, rr, req := prepareRequest(t, "POST", "/", nil)
	form := url.Values{}
	form.Add("Action", "BadRequest")
	req.PostForm = form
	handler := New()

	// Act
	handler.ServeHTTP(rr, req)

	// Asserts
	assert.Equalf(
		http.StatusBadRequest,
		rr.Code,
		"handler returned wrong status code")
}

func TestIndexServerhandler_POST_GoodRequest(t *testing.T) {
	// Arrange
	assert, rr, req := prepareRequest(t, "POST", "/", nil)
	form := url.Values{}
	form.Add("Action", "ListTopics")
	req.PostForm = form
	handler := New()

	// Act
	handler.ServeHTTP(rr, req)

	// Asserts
	assert.Equalf(
		http.StatusOK,
		rr.Code,
		"handler returned wrong status code")
}

func TestIndexServerhandler_POST_GoodRequest_With_URL(t *testing.T) {
	// Arrange
	assert, rr, req := prepareRequest(t, "POST", "/100010001000/local-queue1", nil)
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "local-queue1")
	req.PostForm = form
	handler := New()

	// Act
	handler.ServeHTTP(rr, req)

	// Asserts
	assert.Equalf(
		http.StatusOK,
		rr.Code,
		"handler returned wrong status code")

	// Arrange
	form = url.Values{}
	form.Add("Action", "GetQueueAttributes")
	req.PostForm = form
	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr = httptest.NewRecorder()
	handler = New()

	// Act
	handler.ServeHTTP(rr, req)

	// Asserts
	assert.Equalf(
		http.StatusOK,
		rr.Code,
		"handler returned wrong status code")
}

func TestIndexServerhandler_GET_GoodRequest_Pem_cert(t *testing.T) {
	// Arrange
	assert, rr, req := prepareRequest(t, "GET", "/SimpleNotificationService/100010001000.pem", nil)
	handler := New()

	// Act
	handler.ServeHTTP(rr, req)

	// Asserts
	assert.Equalf(
		http.StatusOK,
		rr.Code,
		"handler returned wrong status code")
}
