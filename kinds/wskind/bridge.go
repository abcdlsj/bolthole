package wskind

import (
	"context"
	"errors"
	"time"

	"github.com/coder/websocket"

	"github.com/abcdlsj/bolthole/mux"
)

// Bridge copies messages both ways between a real WebSocket and a
// ws stream until either side closes.
func Bridge(ctx context.Context, c *websocket.Conn, ws *Conn) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st := ws.Stream()
	go func() {
		defer cancel()
		for {
			op, p, err := ws.Read()
			if err != nil {
				var ce *CloseError
				if errors.As(err, &ce) {
					code := websocket.StatusCode(ce.Code)
					if ce.Code == 1005 || ce.Code < 1000 {
						code = websocket.StatusNormalClosure
					}
					_ = c.Close(code, ce.Reason)
				} else {
					_ = c.Close(websocket.StatusGoingAway, "upstream closed")
				}
				return
			}
			typ := websocket.MessageBinary
			if op == OpText {
				typ = websocket.MessageText
			}
			if err := c.Write(ctx, typ, p); err != nil {
				st.Reset(mux.CodeCancel, "")
				return
			}
		}
	}()
	for {
		typ, p, err := c.Read(ctx)
		if err != nil {
			code := websocket.CloseStatus(err)
			if code == -1 {
				st.Reset(mux.CodeCancel, "")
			} else {
				_ = ws.Close(int(code), "")
				select { // give the other side a moment to close in turn
				case <-st.Done():
				case <-time.After(5 * time.Second):
					st.Reset(mux.CodeCancel, "")
				}
			}
			c.CloseNow()
			return
		}
		op := OpBinary
		if typ == websocket.MessageText {
			op = OpText
		}
		if err := ws.Write(op, p); err != nil {
			_ = c.Close(websocket.StatusGoingAway, "upstream closed")
			return
		}
	}
}
