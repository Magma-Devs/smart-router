package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A per-endpoint `timeout:` EXTENDS the attempt budget; it does not cap it.
//
// The distinction decides whether the attempt-budget change works at all. That field is set on
// nearly every endpoint in the shipped example configs, so a well-meaning change turning `+` into a
// cap would pin those attempts back to a few seconds and make the whole thing a no-op exactly where
// it is deployed — silently, since every test that does not set the field would still pass.
func TestNodeUrlTimeoutExtendsTheBudgetRatherThanCappingIt(t *testing.T) {
	const budget = 30 * time.Second
	const perNode = 5 * time.Second

	deadlineIn := func(url *NodeUrl) time.Duration {
		// A parent with far more room than either value, so the result reflects the helper's own
		// arithmetic rather than CapContextTimeout clamping to the parent.
		parent, cancelParent := context.WithTimeout(context.Background(), time.Hour)
		defer cancelParent()
		ctx, cancel := url.LowerContextTimeoutWithDuration(parent, budget)
		defer cancel()
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "a deadline must be set")
		return time.Until(deadline)
	}

	t.Run("set: added to the budget", func(t *testing.T) {
		got := deadlineIn(&NodeUrl{Url: "http://node", Timeout: perNode})
		assert.InDelta(t, (budget + perNode).Seconds(), got.Seconds(), 1,
			"a per-node timeout must extend the budget to %s; capping here would pin attempts back to %s "+
				"and undo the attempt-budget change on every endpoint that sets it", budget+perNode, perNode)
		assert.Greater(t, got, budget, "it must never shorten the attempt")
	})

	t.Run("unset: the budget alone", func(t *testing.T) {
		got := deadlineIn(&NodeUrl{Url: "http://node"})
		assert.InDelta(t, budget.Seconds(), got.Seconds(), 1)
	})

	t.Run("nil url: the budget alone", func(t *testing.T) {
		var url *NodeUrl
		got := deadlineIn(url)
		assert.InDelta(t, budget.Seconds(), got.Seconds(), 1)
	})

	t.Run("parent still wins when it is shorter", func(t *testing.T) {
		parent, cancelParent := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelParent()
		url := &NodeUrl{Url: "http://node", Timeout: perNode}
		ctx, cancel := url.LowerContextTimeoutWithDuration(parent, budget)
		defer cancel()
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.Less(t, time.Until(deadline), 3*time.Second,
			"an attempt must never outlive the request that owns it")
	})
}
