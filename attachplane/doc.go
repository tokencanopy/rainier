// Package attachplane is the dial-back half of Rainier's terminal attach: the
// pairing table, the runner's dial-back endpoint, and the splice that joins
// one client stream to one runner socket.
//
// It exists because a control plane never dials into a runner (spec rule 3).
// A client's socket arrives at the host, the plane parks it under a fresh
// attach id and asks the session's runner — through the host — to dial back
// carrying that id; the dial-back claims the parked socket and the two are
// spliced until either end stops. Everything above the plane (who may attach,
// in which mode, and whether the session is attachable at all) is the
// application's, and everything below it is the host's sockets.
//
// A Host supplies the three things a plane cannot know: how to authenticate a
// runner's dial-back, how to reach a runner with a command, and this
// replica's own dial-back URL. The state is deliberately replica-local: the
// dial-back's target_url names the exact replica holding the client socket, so
// no other one can be asked to claim a pairing, and a replica dying takes only
// its own live attaches with it — clients re-attach, nothing else is lost. A
// host fronted by a gateway names the gateway's public URL instead and routes
// the dial-back back to this replica itself.
//
// The plane interprets no terminal traffic: a message is decoded to be
// forwarded and for no other reason, and no message, no byte of one, and no
// length of one is ever logged.
package attachplane
