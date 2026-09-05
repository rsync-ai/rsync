## Summary

<!-- What does this PR do? One paragraph or bullet points. -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor / cleanup
- [ ] Documentation
- [ ] Infrastructure / CI

## Testing

<!-- How did you test this? Which scenarios did you verify? -->

- [ ] The Go services you touched compile — this repo has no root Go module, so
      build per service, e.g. `cd api-gateway && go build ./...`
- [ ] Existing tests pass (`make test-all`)
- [ ] Manually tested the affected flows

## Related issues

<!-- Fixes #... -->

## Checklist

- [ ] Every commit is signed off (`git commit -s`) — required by the
      [DCO](https://github.com/rsync-ai/rsync/blob/main/DCO); the DCO check on this PR
      goes red without it. Setup:
      [CONTRIBUTING.md](https://github.com/rsync-ai/rsync/blob/main/CONTRIBUTING.md#developer-certificate-of-origin-dco)
- [ ] No secrets or credentials in the diff
- [ ] No new `.env` files committed
- [ ] Database migration (if any) uses the next sequential prefix in `api-gateway/migrations/`
- [ ] CHANGELOG updated (for user-visible changes)
