package ocsql

// TraceOption allows for functional options.
type TraceOption func(o *TraceOptions)

// TraceOptions holds configuration of our ocsql tracing middleware.
type TraceOptions struct {
	Transaction  bool
	Ping         bool
	RowsNext     bool
	RowsClose    bool
	RowsAffected bool
	LastInsertID bool
	Query        bool
	QueryParams  bool
}

// TraceAll has all tracing options enabled.
var TraceAll = TraceOptions{
	Transaction:  true,
	Ping:         true,
	RowsNext:     true,
	RowsClose:    true,
	RowsAffected: true,
	LastInsertID: true,
	Query:        true,
	QueryParams:  true,
}

// WithOptions sets our ocsql tracing middleware options.
func WithOptions(options TraceOptions) TraceOption {
	return func(o *TraceOptions) {
		*o = options
	}
}

// WithTransaction enables / disables tracing of transaction spans
func WithTransaction(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.Transaction = b
	}
}

// WithPing enables / disables tracing of database Ping calls.
func WithPing(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.Ping = b
	}
}

// WithRowsNext enables / disables tracing of database Rows.Next calls.
func WithRowsNext(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.RowsNext = b
	}
}

// WithRowsClose enables / disables tracing of database Rows.Close calls.
func WithRowsClose(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.RowsClose = b
	}
}

// WithRowsAffected enables / disables tracing of database Result.RowsAffected calls.
func WithRowsAffected(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.RowsAffected = b
	}
}

// WithLastInsertID enables / disables tracing of database Result.LastInsertId calls.
func WithLastInsertID(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.LastInsertID = b
	}
}

// WithQuery enables / disables annotating of database sql queries.
func WithQuery(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.Query = b
	}
}

// WithQueryParams enables / disables tracing of database query parameters.
func WithQueryParams(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.QueryParams = b
	}
}
