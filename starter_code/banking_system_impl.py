import time
from datetime import datetime
import heapq
from banking_system import BankingSystem

class Account:
    def __init__(self, id, balance=0, total_outgoing=0): 
        self.id = id
        self.balance = balance
        self.total_outgoing = total_outgoing
        #self.payments = {}
        #self.transaction_history = {}
        #self.timestamp = datetime.timestamp()
        
    # add amount if transferred to account
    def deposit(self, timestamp: int, amount: int):
        self.balance += amount
        """
        if timestamp not in self.transaction_history.keys():
            self.transaction_history[timestamp] = [amount]
        else: 
            self.transaction_history[timestamp].append(amount)
        """
        return self.balance

    # decrease amount if withdrawn from account
    def withdraw(self, timestamp: int, amount: int): 
        self.balance -= amount
        self.total_outgoing += amount
        """
        if timestamp not in self.transaction_history.keys():
            self.transaction_history[timestamp] = [-1 * amount]
        else: 
            self.transaction_history[timestamp].append(-1 * amount)
        """
        return self.balance     

class BankingSystemImpl(BankingSystem):
    def __init__(self): 
        self.accounts = {}
        self.num_withdraws = 1
        # keep track of cashbacks that need to be processed with priority queue
        self.pending_cashbacks = [] 

    # TODO: implement interface methods here
    def create_account(self, timestamp: int, account_id: str):

        # does not create account if account ID already exists
        if account_id in self.accounts.keys(): 
            return False
        # creates account with unique id and adds account to accounts dictionary
        else:
            self.accounts[account_id] = Account(account_id)
            return True

    def deposit(self, timestamp: int, account_id: str, amount: int) -> int | None:
        # if account exists, adds amount to account balance
        if account_id in self.accounts.keys():
            self.process_cashbacks(timestamp)
            balance = self.accounts[account_id].deposit(timestamp, amount)
            return(balance)
        # does nothing if account does not exist
        else: 
            return None
        
    def transfer(self, timestamp: int, source_account_id: str, target_account_id: str, amount: int) -> int | None:
        # checks that source and target accounts exist
        if (source_account_id not in self.accounts.keys()) or (target_account_id not in self.accounts.keys()):
            return None
        # checks that source and target accounts are not the same
        if source_account_id == target_account_id: 
            return None
        # checks that source account balance is sufficient to make transfer
        if self.accounts[source_account_id].balance < amount: 
            return None
        # if all the checks above are passed, the transfer is successful -> withdraw from source and deposit to target account
        self.process_cashbacks(timestamp)
        self.accounts[target_account_id].deposit(timestamp, amount)
        source_balance = self.accounts[source_account_id].withdraw(timestamp, amount)
        return source_balance
    
    def top_spenders(self, timestamp: int, n: int) -> list[str]:
        # sorts accounts by decreasing total_outgoing amount
        # if there is a tie, sorts by ascending account id
        sorted_accounts = sorted(
            # accounts.values() are Accounts
            self.accounts.values(),
            # sorting accounts
            key=lambda account: (-account.total_outgoing, account.id)
        )
        # returning top n spenders
        return [f"{account.id}({account.total_outgoing})" for account in sorted_accounts[:n]]
        
    def pay(self, timestamp: int, account_id: str, amount: int) -> str | None: 
        ''' — should withdraw the given amount of money from the specified account. All withdraw transactions provide a 2% cashback - 
        2% of the withdrawn amount (rounded down to the nearest integer) will be refunded to the account 
        24 hours after the withdrawal. If the withdrawal is successful (i.e., the account holds sufficient 
        funds to withdraw the given amount), returns a string with a unique identifier for the payment 
        transaction in this format: "payment[ordinal number of withdraws from all accounts]" - 
        e.g., "payment1", "payment2", etc. Additional conditions: 
            o Returns None if account_id doesn't exist. 
            o Returns None if account_id has insufficient funds to perform the payment. 
            o top_spenders should now also account for the total amount of money withdrawn from 
            accounts. 
            o The waiting period for cashback is 24 hours, equal to 24 * 60 * 60 * 1000 = 
            86400000 milliseconds (the unit for timestamps). So, cashback will be processed at 
            timestamp timestamp + 86400000. 
            o When it's time to process cashback for a withdrawal, the amount must be refunded to the 
            account before any other transactions are performed at the relevant timestamp. #priority queue maybe??? '''
        """
        def cashback(self, timestamp: int, amount: int): #might not have to nest function here 
            time.sleep(86400000) #not sure if we want to sleep here
            cashback = round(amount * 0.02)
            self.accounts[account_id].deposit(cashback)
        """
            
        if account_id not in self.accounts.keys():
            return None
        if self.accounts[account_id].balance < amount:
            return None
        
        self.accounts[account_id].withdraw(timestamp, amount)
        payment_id = f"payment{self.num_withdraws}"
        #self.accounts[account_id].deposit(timestamp + 86400000, round(amount*0.02))

        # push (timestamp + 24 hrs, account_id, payment_id, cashback amount) to pending_cashbacks priority queue
        heapq.heappush(self.pending_cashbacks, (timestamp + 86400000, account_id, payment_id, round(amount*0.02)))
        self.num_withdraws += 1

        return payment_id

    def process_cashbacks(self, timestamp: int):
        # check whether first timestamp in priority queue is before current timestamp
        # if it is, then deposit the cashback and take the cashback off of the pending_cashbacks priority queue
        while self.pending_cashbacks and self.pending_cashbacks[0][0] <= timestamp:
            cashback_time, cashback_account_id, payment_id, cashback_amount = heapq.heappop(self.pending_cashbacks)
            self.accounts[cashback_account_id].deposit(timestamp, cashback_amount)    
        
    def get_payment_status(self, timestamp: int, account_id: str, payment: str) -> str | None:
        """
        Should return the status of the payment transaction for the
        given `payment`.
        Specifically:
          * Returns `None` if `account_id` doesn't exist.
          * Returns `None` if the given `payment` doesn't exist for
          the specified account.
          * Returns `None` if the payment transaction was for an
          account with a different identifier from `account_id`.
          * Returns a string representing the payment status:
          `"IN_PROGRESS"` or `"CASHBACK_RECEIVED"`.
        """
        # check whether account_id exists, return None if not
        if account_id not in self.accounts.keys():
            return None
        # iterate over pending cashbacks to find if there is a matching account/payment id for cashbacks in progress
        # return None if given payment id does not exist for specified account
        # return None if payment transaction was for a different account_id

        for cashback in self.pending_cashbacks: 
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback 
            if account_id == cashback_account_id and payment == cashback_payment_id:
                if cashback_time > timestamp:
                    return "IN_PROGRESS"
                else: 
                    return "CASHBACK_RECEIVED"
        
        return None
        

        
        