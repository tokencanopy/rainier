package control

import "errors"

// The closed error set of the control contract. Every operation in this
// package reports failure through one of these seven sentinels; ports may
// wrap a sentinel with safe context but must not surface SQL, credentials,
// provider responses, filesystem paths, terminal output, or session content
// in that context. Callers branch with errors.Is and never inspect the
// wrapped message text.
//
// HTTP adapters map these centrally to status codes; this package carries no
// status code and no public error-envelope text.
var (
	// ErrInvalid reports a command or scope that is malformed: an empty or
	// unknown ID, an unknown actor kind or execution mode, a scope missing
	// its hosted region or cell, or a contradictory create (an environment
	// and a scratch spec together). It is the one non-disclosing answer for
	// input that cannot be authorized against anything.
	ErrInvalid = errors.New("control: invalid")

	// ErrDenied reports that the current authorizer refused the operation.
	// The resource may or may not exist; ErrDenied does not say which.
	ErrDenied = errors.New("control: denied")

	// ErrNotFound reports a lookup that found nothing. It is also the
	// non-disclosing answer for a resource outside the authoritative
	// workspace: a caller asking about another workspace's session learns
	// "not found", never "that workspace exists and has this session".
	ErrNotFound = errors.New("control: not found")

	// ErrConflict reports a guarded transition whose current state was not in
	// the allowed from-list, a name already held, or a contradictory request.
	ErrConflict = errors.New("control: conflict")

	// ErrStale reports an operation whose authoritative generation or setup
	// hash no longer matches the resource's current one: a runner event from
	// a superseded placement generation, or a snapshot built from an edited
	// environment.
	ErrStale = errors.New("control: stale")

	// ErrUnavailable reports a dependency that is temporarily unusable: a
	// runner with no control connection, a store that cannot answer.
	ErrUnavailable = errors.New("control: unavailable")

	// ErrUnsupported reports an operation or capability the current build
	// does not implement.
	ErrUnsupported = errors.New("control: unsupported")
)
