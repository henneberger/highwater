from highwater import activity, execute_activity, wait_condition, workflow


@activity.defn
def charge(card: str, amount: int) -> dict:
    return {"card": card[-4:], "charged": amount}


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.approved = False
        self.amount = 0

    @workflow.run
    async def run(self, card: str, amount: int) -> dict:
        self.amount = amount
        await wait_condition(lambda: self.approved)
        return await execute_activity(charge, card, self.amount)

    @workflow.signal
    def approve(self) -> None:
        self.approved = True

    @workflow.query
    def state(self) -> dict:
        return {"approved": self.approved, "amount": self.amount}

    @workflow.update
    def change_amount(self, amount: int) -> int:
        if amount <= 0:
            raise ValueError("amount must be positive")
        self.amount = amount
        return amount
