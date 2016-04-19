package listener_test

import (
	"net"
	"trafficcontroller/listener"

	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
	"trafficcontroller/marshaller"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"

	"github.com/cloudfoundry/loggregatorlib/loggertesthelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("WebsocketListener", func() {
	var (
		ts                      *httptest.Server
		messageChan, outputChan chan []byte
		stopChan                chan struct{}
		l                       listener.Listener
		fh                      *fakeHandler
		converter               func([]byte) ([]byte, error)
	)
	const (
		readTimeout      = 500 * time.Millisecond
		handshakeTimeout = 500 * time.Millisecond
	)

	BeforeEach(func() {
		messageChan = make(chan []byte)
		outputChan = make(chan []byte, 10)
		stopChan = make(chan struct{})
		fh = &fakeHandler{messages: messageChan}
		ts = httptest.NewUnstartedServer(fh)
		converter = func(d []byte) ([]byte, error) { return d, nil }
	})

	JustBeforeEach(func() {
		l = listener.NewWebsocket(
			marshaller.DropsondeLogMessage,
			converter,
			readTimeout,
			handshakeTimeout,
			loggertesthelper.Logger(),
		)
	})

	AfterEach(func() {
		select {
		case <-messageChan:
			// already closed
		default:
			close(messageChan)
		}
		ts.Close()
	})

	Context("when the server is not running", func() {
		It("should error when connecting", func(done Done) {
			err := l.Start("ws://localhost:1234", "myApp", outputChan, stopChan)
			Expect(err).To(HaveOccurred())
			close(done)
		}, 2)
	})

	Context("when the converter returns nil messages", func() {
		BeforeEach(func() {
			converter = func([]byte) ([]byte, error) { return nil, nil }
		})

		JustBeforeEach(func() {
			ts.Start()
			Eventually(func() bool {
				resp, _ := http.Head(fmt.Sprintf("http://%s", ts.Listener.Addr()))
				return resp != nil && resp.StatusCode == http.StatusOK
			}).Should(BeTrue())
		})

		It("should ignore nil messages received from the server", func() {
			go l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)

			messageChan <- []byte("ignored")

			Consistently(outputChan).ShouldNot(Receive())
		})
	})

	Context("when the server is running", func() {
		JustBeforeEach(func() {
			ts.Start()
			Eventually(func() bool {
				resp, _ := http.Head(fmt.Sprintf("http://%s", ts.Listener.Addr()))
				return resp != nil && resp.StatusCode == http.StatusOK
			}).Should(BeTrue())
		})

		It("should connect to a websocket", func(done Done) {
			doneWaiting := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				err := l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
				Expect(err).NotTo(HaveOccurred())
				close(doneWaiting)
			}()
			close(stopChan)
			Eventually(doneWaiting).Should(BeClosed())
			close(done)
		})

		It("should output messages recieved from the server", func() {
			go l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)

			message := []byte("hello world")
			messageChan <- message

			var receivedMessage []byte
			Eventually(outputChan).Should(Receive(&receivedMessage))
			Expect(receivedMessage).To(Equal(message))
		})

		It("should not send errors when client requests close without issue", func() {
			doneWaiting := make(chan struct{})
			go func() {
				l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
				close(doneWaiting)
			}()
			close(stopChan)
			Eventually(doneWaiting).Should(BeClosed())

			Consistently(outputChan).Should(BeEmpty())
		})

		It("should not send errors when server closes connection without issue", func() {
			doneWaiting := make(chan struct{})
			go func() {
				l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
				close(doneWaiting)
			}()
			close(messageChan)

			Eventually(doneWaiting).Should(BeClosed())
			Consistently(outputChan).Should(BeEmpty())
		})

		It("should stop all goroutines when done", func() {
			doneWaiting := make(chan struct{})
			go func() {
				l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
				close(doneWaiting)
			}()
			close(stopChan)
			Consistently(outputChan).ShouldNot(BeClosed())
			Eventually(doneWaiting).Should(BeClosed())
		})

		It("should stop all goroutines when server returns an error", func(done Done) {
			doneWaiting := make(chan struct{})
			go func() {
				l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
				close(doneWaiting)
			}()

			// Ensure listener is up by sending message through
			message := []byte("hello world")
			messageChan <- message
			outMessage := <-outputChan
			Expect(outMessage).To(Equal(message))

			// Take server down to cause listener to go down
			close(messageChan)
			Consistently(outputChan).ShouldNot(BeClosed())
			Consistently(stopChan).ShouldNot(BeClosed())
			Eventually(doneWaiting).Should(BeClosed())
			close(done)
		})
	})

	Context("when the server is slow", func() {
		JustBeforeEach(func() {
			ts.Start()
			Eventually(func() bool {
				resp, _ := http.Head(fmt.Sprintf("http://%s", ts.Listener.Addr()))
				return resp != nil && resp.StatusCode == http.StatusOK
			}).Should(BeTrue())
		})

		Context("with a read timeout set", func() {
			It("does not wait forever", func(done Done) {
				err := l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
				Expect(err).To(HaveOccurred())

				close(done)
			})

			It("sends an error message to the channel", func(done Done) {
				l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
				var msgData []byte
				Eventually(outputChan).Should(Receive(&msgData))

				var envelope events.Envelope
				err := proto.Unmarshal(msgData, &envelope)
				Expect(err).ToNot(HaveOccurred())
				Expect(envelope.GetLogMessage().GetSourceType()).To(Equal("DOP"))
				Expect(string(envelope.GetLogMessage().GetMessage())).To(Equal("WebsocketListener.Start: Timed out listening to a doppler server after 500ms"))
				close(done)
			})
		})

		Context("without a read timeout", func() {
			It("waits for messages to come in", func() {
				converter := func(d []byte) ([]byte, error) { return d, nil }
				l = listener.NewWebsocket(marshaller.DropsondeLogMessage, converter, 0, handshakeTimeout, loggertesthelper.Logger())

				go l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)

				go func() {
					time.Sleep(750 * time.Millisecond)
					message := []byte("hello world")
					messageChan <- message
				}()

				var msgData []byte
				Eventually(outputChan).Should(Receive(&msgData))
				Expect(msgData).To(BeEquivalentTo("hello world"))
			})

			It("responds to stopChan closure in a reasonable time", func(done Done) {
				converter := func(d []byte) ([]byte, error) { return d, nil }
				l = listener.NewWebsocket(marshaller.DropsondeLogMessage, converter, 0, handshakeTimeout, loggertesthelper.Logger())

				go func() {
					l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
					close(done)
				}()
				time.Sleep(10 * time.Millisecond)
				close(stopChan)
			})
		})
	})

	Context("when the server has errors", func() {
		JustBeforeEach(func() {
			ts.Start()
			go l.Start(fmt.Sprintf("ws://%s", ts.Listener.Addr()), "myApp", outputChan, stopChan)
			fh.CloseAbruptly()
		})

		It("should send an error message to the channel", func(done Done) {
			msgData := <-outputChan
			var envelope events.Envelope
			err := proto.Unmarshal(msgData, &envelope)
			Expect(err).ToNot(HaveOccurred())
			Expect(envelope.GetLogMessage().GetSourceType()).To(Equal("DOP"))
			Expect(envelope.GetLogMessage().GetMessage()).To(BeEquivalentTo("WebsocketListener.Start: Error connecting to a doppler server"))
			close(done)
		})
	})

	Context("with a sever that is listening but doesn't accept the connection", func() {
		var deadServer net.Listener

		BeforeEach(func() {
			var err error
			deadServer, err = net.Listen("tcp", ":12345")
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			deadServer.Close()
		})

		It("returns a io timeout error", func(done Done) {
			defer close(done)
			err := l.Start(fmt.Sprintf("ws://%s", "localhost:12345"), "myApp", outputChan, stopChan)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("i/o timeout"))
		})

		Context("without a handshake timeout", func() {
			var localListener listener.Listener

			BeforeEach(func() {
				// we needed to create a local listerer to avoid a data race since this
				// go routine can not be cleaned up
				localListener = listener.NewWebsocket(
					marshaller.DropsondeLogMessage,
					converter,
					readTimeout,
					0,
					loggertesthelper.Logger(),
				)
			})

			It("doesn't return a timeout error", func() {
				errCh := make(chan error)

				go func() {
					errCh <- localListener.Start(fmt.Sprintf("ws://%s", "localhost:12345"), "myApp", nil, nil)
				}()

				Consistently(errCh).ShouldNot(Receive())
			})
		})
	})
})

type fakeHandler struct {
	messages   chan []byte
	lastWSConn *websocket.Conn
	sync.Mutex
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "HEAD" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	ws, err := websocket.Upgrade(w, r, nil, 0, 0)
	if _, ok := err.(websocket.HandshakeError); ok {
		http.Error(w, "Not a websocket handshake", 400)
		return
	} else if err != nil {
		log.Println(err)
		return
	}
	defer ws.Close()
	defer ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Time{})
	f.Lock()
	f.lastWSConn = ws
	f.Unlock()

	go func() {
		ws.ReadMessage()

		select {
		case <-f.messages:
		default:
			close(f.messages)
		}
	}()

	for msg := range f.messages {
		if err := ws.WriteMessage(websocket.BinaryMessage, msg); err != nil {
			return
		}
	}
}

func (f *fakeHandler) Close() {
	close(f.messages)
}

func (f *fakeHandler) CloseAbruptly() {
	Eventually(f.lastConn).ShouldNot(BeNil())
	f.lastConn().Close()
}

func (f *fakeHandler) lastConn() *websocket.Conn {
	f.Lock()
	defer f.Unlock()
	return f.lastWSConn
}
