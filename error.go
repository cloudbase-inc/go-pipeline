package pipeline

import "errors"

type abortError struct {
	err error
}

func (e *abortError) Error() string {
	return e.err.Error()
}

func (e *abortError) Unwrap() error {
	return e.err
}

// 全体のパイプラインを中止すべきクリティカルなエラーが発生した場合は、このエラーを返してください
func AbortError(err error) error {
	return &abortError{err}
}

func IsAbortError(err error) bool {
	var e *abortError
	return errors.As(err, &e)
}
