package logger

import (
	"context"

	"go.uber.org/zap"
)

type ZapLogger struct {
	sugar *zap.SugaredLogger
}

func NewZapLogger() (*ZapLogger, error) {
	zapLogger, err := zap.NewDevelopment()
	//zapLogger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	sugar := zapLogger.Sugar()
	return &ZapLogger{sugar: sugar}, nil
}

func (zl *ZapLogger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	zl.sugar.Infow(msg, keysAndValues...)
}

func (zl *ZapLogger) Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	zl.sugar.Errorw(msg, keysAndValues...)
}
