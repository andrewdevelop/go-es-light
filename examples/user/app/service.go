package app

import (
	"context"
	"errors"

	"go-es-light/es"
	"go-es-light/examples/user/domain"
)

// UserService is the use-case layer: each method loads history, replays it,
// calls exactly one aggregate method, saves, and returns using the
// aggregate's in-memory state. It never waits for ReadModel or Notifier to
// catch up — those run asynchronously off the event stream (see the
// adapters and main.go's projector goroutine).
type UserService struct {
	repo *UserRepository
}

func NewUserService(repo *UserRepository) *UserService {
	return &UserService{repo: repo}
}

func (s *UserService) RegisterUser(ctx context.Context, cmd RegisterUserCommand) (UserView, error) {
	user, err := s.repo.Load(ctx, cmd.UserID)
	if errors.Is(err, es.ErrAggregateNotFound) {
		user = &domain.User{}
		user.SetID(cmd.UserID)
	} else if err != nil {
		return UserView{}, err
	}

	if err := user.Register(cmd.Email, cmd.Name); err != nil {
		return UserView{}, err
	}
	if err := s.repo.Save(ctx, user); err != nil {
		return UserView{}, err
	}
	return toView(user), nil
}

func (s *UserService) ChangeEmail(ctx context.Context, cmd ChangeEmailCommand) (UserView, error) {
	user, err := s.repo.Load(ctx, cmd.UserID)
	if err != nil {
		return UserView{}, err
	}
	if err := user.ChangeEmail(cmd.Email); err != nil {
		return UserView{}, err
	}
	if err := s.repo.Save(ctx, user); err != nil {
		return UserView{}, err
	}
	return toView(user), nil
}

func (s *UserService) ChangeName(ctx context.Context, cmd ChangeNameCommand) (UserView, error) {
	user, err := s.repo.Load(ctx, cmd.UserID)
	if err != nil {
		return UserView{}, err
	}
	if err := user.ChangeName(cmd.Name); err != nil {
		return UserView{}, err
	}
	if err := s.repo.Save(ctx, user); err != nil {
		return UserView{}, err
	}
	return toView(user), nil
}

func toView(u *domain.User) UserView {
	return UserView{ID: u.GetID(), Email: u.Email, Name: u.Name, Version: u.GetVersion()}
}
