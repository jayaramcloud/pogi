package generic_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/nrndev/pogi/internal/aws/generic"
)

const costAcct = "000000000000"

func TestBudgets(t *testing.T) {
	srv, cfg := startCost(t, "budgets", "s3", "sns", "sqs")
	ctx := context.Background()
	bc := budgets.NewFromConfig(cfg)

	// a topic with a queue behind it, for the alerts
	sn, sq := sns.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	topic, err := sn.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("alerts")})
	if err != nil {
		t.Fatal(err)
	}
	q, _ := sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("alerts-q")})
	attrs, _ := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"}})
	if _, err := sn.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(attrs.Attributes["QueueArn"])}); err != nil {
		t.Fatal(err)
	}
	notification := types.Notification{NotificationType: types.NotificationTypeActual, ComparisonOperator: types.ComparisonOperatorGreaterThan, Threshold: 100}
	create := &budgets.CreateBudgetInput{AccountId: aws.String(costAcct), Budget: &types.Budget{BudgetName: aws.String("dev"), BudgetType: types.BudgetTypeCost,
		TimeUnit: types.TimeUnitDaily, BudgetLimit: &types.Spend{Amount: aws.String("0.00001"), Unit: aws.String("USD")},
		CostFilters: map[string][]string{"Service": {"Amazon Simple Storage Service"}}},
		NotificationsWithSubscribers: []types.NotificationWithSubscribers{{Notification: &notification,
			Subscribers: []types.Subscriber{{SubscriptionType: types.SubscriptionTypeSns, Address: topic.TopicArn}}}},
		ResourceTags: []types.ResourceTag{{Key: aws.String("team"), Value: aws.String("a")}}}
	if _, err := bc.CreateBudget(ctx, create); err != nil {
		t.Fatal(err)
	}
	var dup *types.DuplicateRecordException
	if _, err := bc.CreateBudget(ctx, create); !errors.As(err, &dup) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	d, err := bc.DescribeBudget(ctx, &budgets.DescribeBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
	if err != nil || aws.ToString(d.Budget.BudgetLimit.Amount) != "0.00001" || d.Budget.TimeUnit != types.TimeUnitDaily || d.Budget.TimePeriod == nil ||
		aws.ToString(d.Budget.CalculatedSpend.ActualSpend.Amount) != "0" || d.Budget.CostTypes == nil || !aws.ToBool(d.Budget.CostTypes.IncludeTax) {
		t.Fatalf("describe: %v %+v", err, d)
	}
	list, err := bc.DescribeBudgets(ctx, &budgets.DescribeBudgetsInput{AccountId: aws.String(costAcct)})
	if err != nil || len(list.Budgets) != 1 {
		t.Fatalf("list: %v %+v", err, list)
	}
	arn := "arn:aws:budgets::" + costAcct + ":budget/dev"
	tags, err := bc.ListTagsForResource(ctx, &budgets.ListTagsForResourceInput{ResourceARN: aws.String(arn)})
	if err != nil || len(tags.ResourceTags) != 1 || aws.ToString(tags.ResourceTags[0].Key) != "team" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := bc.TagResource(ctx, &budgets.TagResourceInput{ResourceARN: aws.String(arn), ResourceTags: []types.ResourceTag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.UntagResource(ctx, &budgets.UntagResourceInput{ResourceARN: aws.String(arn), ResourceTagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	tags, _ = bc.ListTagsForResource(ctx, &budgets.ListTagsForResourceInput{ResourceARN: aws.String(arn)})
	if len(tags.ResourceTags) != 1 || aws.ToString(tags.ResourceTags[0].Key) != "env" {
		t.Fatalf("tags after: %+v", tags.ResourceTags)
	}

	// S3 spend crosses the limit: the check puts the notification in alarm and tells the topic, once
	tick := func() { srv.Hub.Service("budgets").(*generic.Service).Tick(time.Now()) }
	state := func() types.NotificationState {
		n, err := bc.DescribeNotificationsForBudget(ctx, &budgets.DescribeNotificationsForBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
		if err != nil || len(n.Notifications) == 0 {
			t.Fatalf("notifications: %v %+v", err, n)
		}
		return n.Notifications[0].NotificationState
	}
	tick()
	if state() != types.NotificationStateOk {
		t.Fatal("alarm before any spend")
	}
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("spend")})
	for _, k := range []string{"a", "b", "c"} {
		if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("spend"), Key: aws.String(k), Body: bytes.NewReader([]byte("x"))}); err != nil {
			t.Fatal(err)
		}
	}
	d, _ = bc.DescribeBudget(ctx, &budgets.DescribeBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
	if got := aws.ToString(d.Budget.CalculatedSpend.ActualSpend.Amount); got != "0.00002" {
		t.Fatalf("actual spend %s, want 4 requests at the tier 1 rate", got)
	}
	if costF(t, d.Budget.CalculatedSpend.ForecastedSpend.Amount) < 0.00002 {
		t.Fatalf("forecast %s", *d.Budget.CalculatedSpend.ForecastedSpend.Amount)
	}
	tick()
	tick()
	if state() != types.NotificationStateAlarm {
		t.Fatal("no alarm after the spend")
	}
	msgs, err := sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
	if err != nil || len(msgs.Messages) != 1 || !strings.Contains(aws.ToString(msgs.Messages[0].Body), "dev") || !strings.Contains(aws.ToString(msgs.Messages[0].Body), "exceeded") {
		t.Fatalf("alert: %v %+v", err, msgs)
	}

	// performance history: today's period holds the spend against the limit
	h, err := bc.DescribeBudgetPerformanceHistory(ctx, &budgets.DescribeBudgetPerformanceHistoryInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
	if err != nil {
		t.Fatal(err)
	}
	rows := h.BudgetPerformanceHistory.BudgetedAndActualAmountsList
	if len(rows) != 14 || aws.ToString(rows[13].BudgetedAmount.Amount) != "0.00001" || aws.ToString(rows[13].ActualAmount.Amount) != "0.00002" || aws.ToString(rows[0].ActualAmount.Amount) != "0" {
		t.Fatalf("history: %d rows, last %+v", len(rows), rows[len(rows)-1])
	}

	// raising the limit puts it back to ok
	if _, err := bc.UpdateBudget(ctx, &budgets.UpdateBudgetInput{AccountId: aws.String(costAcct), NewBudget: &types.Budget{BudgetName: aws.String("dev"), BudgetType: types.BudgetTypeCost,
		TimeUnit: types.TimeUnitDaily, BudgetLimit: &types.Spend{Amount: aws.String("1"), Unit: aws.String("USD")}, CostFilters: map[string][]string{"Service": {"Amazon Simple Storage Service"}}}}); err != nil {
		t.Fatal(err)
	}
	tick()
	if state() != types.NotificationStateOk {
		t.Fatal("still in alarm under the new limit")
	}

	// notifications and subscribers
	second := types.Notification{NotificationType: types.NotificationTypeForecasted, ComparisonOperator: types.ComparisonOperatorGreaterThan, Threshold: 80}
	email := types.Subscriber{SubscriptionType: types.SubscriptionTypeEmail, Address: aws.String("ops@example.com")}
	if _, err := bc.CreateNotification(ctx, &budgets.CreateNotificationInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &second, Subscribers: []types.Subscriber{email}}); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.CreateNotification(ctx, &budgets.CreateNotificationInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &second, Subscribers: []types.Subscriber{email}}); !errors.As(err, &dup) {
		t.Fatalf("duplicate notification: %T %v", err, err)
	}
	other := types.Subscriber{SubscriptionType: types.SubscriptionTypeEmail, Address: aws.String("dev@example.com")}
	if _, err := bc.CreateSubscriber(ctx, &budgets.CreateSubscriberInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &second, Subscriber: &other}); err != nil {
		t.Fatal(err)
	}
	subs, err := bc.DescribeSubscribersForNotification(ctx, &budgets.DescribeSubscribersForNotificationInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &second})
	if err != nil || len(subs.Subscribers) != 2 {
		t.Fatalf("subscribers: %v %+v", err, subs)
	}
	renamed := types.Subscriber{SubscriptionType: types.SubscriptionTypeEmail, Address: aws.String("lead@example.com")}
	if _, err := bc.UpdateSubscriber(ctx, &budgets.UpdateSubscriberInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &second, OldSubscriber: &other, NewSubscriber: &renamed}); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.DeleteSubscriber(ctx, &budgets.DeleteSubscriberInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &second, Subscriber: &email}); err != nil {
		t.Fatal(err)
	}
	var invalid *types.InvalidParameterException
	if _, err := bc.DeleteSubscriber(ctx, &budgets.DeleteSubscriberInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &second, Subscriber: &renamed}); !errors.As(err, &invalid) {
		t.Fatalf("last subscriber: %T %v", err, err)
	}
	moved := types.Notification{NotificationType: types.NotificationTypeForecasted, ComparisonOperator: types.ComparisonOperatorGreaterThan, Threshold: 90}
	if _, err := bc.UpdateNotification(ctx, &budgets.UpdateNotificationInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), OldNotification: &second, NewNotification: &moved}); err != nil {
		t.Fatal(err)
	}
	all, err := bc.DescribeBudgetNotificationsForAccount(ctx, &budgets.DescribeBudgetNotificationsForAccountInput{AccountId: aws.String(costAcct)})
	if err != nil || len(all.BudgetNotificationsForAccount) != 1 || len(all.BudgetNotificationsForAccount[0].Notifications) != 2 {
		t.Fatalf("account notifications: %v %+v", err, all)
	}
	if _, err := bc.DeleteNotification(ctx, &budgets.DeleteNotificationInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), Notification: &moved}); err != nil {
		t.Fatal(err)
	}

	// actions
	act, err := bc.CreateBudgetAction(ctx, &budgets.CreateBudgetActionInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), NotificationType: types.NotificationTypeActual,
		ActionType: types.ActionTypeIam, ActionThreshold: &types.ActionThreshold{ActionThresholdType: types.ThresholdTypePercentage, ActionThresholdValue: 90},
		Definition:       &types.Definition{IamActionDefinition: &types.IamActionDefinition{PolicyArn: aws.String("arn:aws:iam::aws:policy/AWSDenyAll"), Roles: []string{"r"}}},
		ExecutionRoleArn: aws.String("arn:aws:iam::" + costAcct + ":role/budget"), ApprovalModel: types.ApprovalModelManual,
		Subscribers: []types.Subscriber{email}})
	if err != nil || aws.ToString(act.ActionId) == "" {
		t.Fatalf("action: %v %+v", err, act)
	}
	da, err := bc.DescribeBudgetAction(ctx, &budgets.DescribeBudgetActionInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), ActionId: act.ActionId})
	if err != nil || da.Action.Status != types.ActionStatusStandby || da.Action.ActionType != types.ActionTypeIam || aws.ToString(da.Action.Definition.IamActionDefinition.PolicyArn) == "" {
		t.Fatalf("describe action: %v %+v", err, da)
	}
	if _, err := bc.UpdateBudgetAction(ctx, &budgets.UpdateBudgetActionInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), ActionId: act.ActionId, ApprovalModel: types.ApprovalModelAuto}); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.ExecuteBudgetAction(ctx, &budgets.ExecuteBudgetActionInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), ActionId: act.ActionId, ExecutionType: types.ExecutionTypeApproveBudgetAction}); err != nil {
		t.Fatal(err)
	}
	fa, err := bc.DescribeBudgetActionsForBudget(ctx, &budgets.DescribeBudgetActionsForBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
	if err != nil || len(fa.Actions) != 1 || fa.Actions[0].Status != types.ActionStatusExecutionSuccess || fa.Actions[0].ApprovalModel != types.ApprovalModelAuto {
		t.Fatalf("actions for budget: %v %+v", err, fa)
	}
	hist, err := bc.DescribeBudgetActionHistories(ctx, &budgets.DescribeBudgetActionHistoriesInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), ActionId: act.ActionId})
	if err != nil || len(hist.ActionHistories) != 3 || hist.ActionHistories[2].EventType != types.EventTypeExecuteAction {
		t.Fatalf("histories: %v %+v", err, hist)
	}
	if acc, err := bc.DescribeBudgetActionsForAccount(ctx, &budgets.DescribeBudgetActionsForAccountInput{AccountId: aws.String(costAcct)}); err != nil || len(acc.Actions) != 1 {
		t.Fatalf("actions for account: %v %+v", err, acc)
	}
	if _, err := bc.DeleteBudgetAction(ctx, &budgets.DeleteBudgetActionInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), ActionId: act.ActionId}); err != nil {
		t.Fatal(err)
	}
	var nf *types.NotFoundException
	if _, err := bc.DescribeBudgetAction(ctx, &budgets.DescribeBudgetActionInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev"), ActionId: act.ActionId}); !errors.As(err, &nf) {
		t.Fatalf("deleted action: %T %v", err, err)
	}

	// delete, then not found
	if _, err := bc.DeleteBudget(ctx, &budgets.DeleteBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")}); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		func() error {
			_, err := bc.DescribeBudget(ctx, &budgets.DescribeBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
			return err
		}(),
		func() error {
			_, err := bc.DescribeNotificationsForBudget(ctx, &budgets.DescribeNotificationsForBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
			return err
		}(),
		func() error {
			_, err := bc.DeleteBudget(ctx, &budgets.DeleteBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("dev")})
			return err
		}(),
	} {
		if !errors.As(err, &nf) {
			t.Fatalf("deleted budget: %T %v", err, err)
		}
	}
}

// Budgets, their notifications and their actions survive a snapshot: the state holds only JSON.
func TestBudgetsPersistence(t *testing.T) {
	srv, cfg := startCost(t, "budgets")
	ctx := context.Background()
	bc := budgets.NewFromConfig(cfg)
	n := types.Notification{NotificationType: types.NotificationTypeActual, ComparisonOperator: types.ComparisonOperatorGreaterThan, Threshold: 90}
	if _, err := bc.CreateBudget(ctx, &budgets.CreateBudgetInput{AccountId: aws.String(costAcct), Budget: &types.Budget{BudgetName: aws.String("keep"), BudgetType: types.BudgetTypeCost,
		TimeUnit: types.TimeUnitMonthly, BudgetLimit: &types.Spend{Amount: aws.String("10"), Unit: aws.String("USD")}},
		NotificationsWithSubscribers: []types.NotificationWithSubscribers{{Notification: &n, Subscribers: []types.Subscriber{{SubscriptionType: types.SubscriptionTypeEmail, Address: aws.String("a@b.c")}}}}}); err != nil {
		t.Fatal(err)
	}
	snap, err := srv.Hub.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	srv.Hub.Reset()
	if _, err := bc.DescribeBudget(ctx, &budgets.DescribeBudgetInput{AccountId: aws.String(costAcct), BudgetName: aws.String("keep")}); err == nil {
		t.Fatal("budget survived a reset")
	}
	if err := srv.Hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	subs, err := bc.DescribeSubscribersForNotification(ctx, &budgets.DescribeSubscribersForNotificationInput{AccountId: aws.String(costAcct), BudgetName: aws.String("keep"), Notification: &n})
	if err != nil || len(subs.Subscribers) != 1 || aws.ToString(subs.Subscribers[0].Address) != "a@b.c" {
		t.Fatalf("after restore: %v %+v", err, subs)
	}
}
