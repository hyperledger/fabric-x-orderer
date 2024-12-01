package testutil

type SilentLogger struct{}

func (s *SilentLogger) Info(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Infoln(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Infof(format string, args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Warning(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Warningln(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Warningf(format string, args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Error(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Errorln(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Errorf(format string, args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Fatal(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Fatalln(args ...any) {
	// TODO implement me
}

func (s *SilentLogger) Fatalf(format string, args ...any) {
	// TODO implement me
}

func (s *SilentLogger) V(l int) bool {
	return false
}
