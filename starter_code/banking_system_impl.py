import time
from datetime import datetime
import heapq
import math
from banking_system import BankingSystem

class Account:
    def __init__(self, timestamp, id, balance=0, total_outgoing=0): 
        self.creation_timestamp = timestamp
        self.id = id
        self.balance = balance
        self.balance_history = {timestamp: balance}
        self.total_outgoing = total_outgoing
        
    # add amount if transferred to account
    def deposit(self, timestamp: int, amount: int):
        self.balance += amount
        self.balance_history[timestamp] = self.balance
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
        self.balance_history[timestamp] = self.balance
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
        self.completed_cashbacks = []

    # TODO: implement interface methods here
    def create_account(self, timestamp: int, account_id: str):

        # does not create account if account ID already exists
        if account_id in self.accounts.keys(): 
            return False
        # creates account with unique id and adds account to accounts dictionary
        else:
            self.accounts[account_id] = Account(timestamp, account_id)
            
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
        self.process_cashbacks(timestamp)
        if self.accounts[account_id].balance < amount:
            return None
        
        self.accounts[account_id].withdraw(timestamp, amount)
        payment_id = f"payment{self.num_withdraws}"
        #self.accounts[account_id].deposit(timestamp + 86400000, round(amount*0.02))

        # push (timestamp + 24 hrs, account_id, payment_id, cashback amount) to pending_cashbacks priority queue
        heapq.heappush(self.pending_cashbacks, (timestamp + 86400000, account_id, payment_id, math.floor(amount*0.02)))
        self.num_withdraws += 1

        return payment_id

    def process_cashbacks(self, timestamp: int):
        # check whether first timestamp in priority queue is before current timestamp
        # if it is, then deposit the cashback and take the cashback off of the pending_cashbacks priority queue
        while self.pending_cashbacks and self.pending_cashbacks[0][0] <= timestamp:
            cashback_time, cashback_account_id, payment_id, cashback_amount = heapq.heappop(self.pending_cashbacks)
            self.accounts[cashback_account_id].deposit(cashback_time, cashback_amount)   
            self.completed_cashbacks.append([cashback_time, cashback_account_id, payment_id, cashback_amount])
        
        
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
        self.process_cashbacks(timestamp)
        # iterate over pending cashbacks to find if there is a matching account/payment id for cashbacks in progress
        # return None if given payment id does not exist for specified account
        # return None if payment transaction was for a different account_id

        for cashback in self.pending_cashbacks: 
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback 
            if account_id == cashback_account_id and payment == cashback_payment_id:
                if cashback_time > timestamp:
                    return "IN_PROGRESS"
                
        for cashback in self.completed_cashbacks:
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback
            if account_id == cashback_account_id and payment == cashback_payment_id:
                return "CASHBACK_RECEIVED"
        
        return None
    
    def merge_cashbacks(self, timestamp: int, account_id_1: str, account_id_2: str):
        for i, cashback in enumerate(self.pending_cashbacks): 
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback 
            if cashback_account_id == account_id_2:
                updated_cashback = (cashback_time, account_id_1, cashback_payment_id, cashback_amount)
                self.pending_cashbacks[i] = updated_cashback
                heapq.heapify(self.pending_cashbacks)

        for i, cashback in enumerate(self.completed_cashbacks): 
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback 
            if cashback_account_id == account_id_2:
                updated_cashback = (cashback_time, account_id_1, cashback_payment_id, cashback_amount)
                self.completed_cashbacks[i] = updated_cashback
                heapq.heapify(self.completed_cashbacks)            
        
    
    def merge_accounts(self, timestamp: int, account_id_1: str, account_id_2: str) -> bool:
        """
        Should merge `account_id_2` into the `account_id_1`.
        Returns `True` if accounts were successfully merged, or
        `False` otherwise.
        Specifically:
          * Returns `False` if `account_id_1` is equal to
          `account_id_2`.
          * Returns `False` if `account_id_1` or `account_id_2`
          doesn't exist.
          * All pending cashback refunds for `account_id_2` should
          still be processed, but refunded to `account_id_1` instead.
          * After the merge, it must be possible to check the status
          of payment transactions for `account_id_2` with payment
          identifiers by replacing `account_id_2` with `account_id_1`.
          * The balance of `account_id_2` should be added to the
          balance for `account_id_1`.
          * `top_spenders` operations should recognize merged accounts
          - the total outgoing transactions for merged accounts should
          be the sum of all money transferred and/or withdrawn in both
          accounts.
          * `account_id_2` should be removed from the system after the
          merge.
        """
        if account_id_1 == account_id_2:
            return False
        if (account_id_1 not in self.accounts.keys()) or (account_id_2 not in self.accounts.keys()):
            return False
        # update balance of account 1 to include account 2 balance
        self.accounts[account_id_1].deposit(timestamp, self.accounts[account_id_2].balance)
        
        # update total outgoing of account 1 to include account 2 total outgoing
        self.accounts[account_id_1].total_outgoing += self.accounts[account_id_2].total_outgoing
        # update the balance history of account 1 to include account 2 balance history
        #self.accounts[account_id_1].balance_history.update(self.accounts[account_id_2].balance_history)
        
        #for time, balance in self.accounts[account_id_1].balance_history.items(): 
            #if time <= timestamp
                #self.accounts[account_id_2].balance_history[time] = balance
                
        # call to different function for updating pending_cashbacks and completed_cashbacks with account id 1 for account id 2
        self.merge_cashbacks(timestamp, account_id_1, account_id_2)
        # take account 2 id out of the accounts dictionary
        self.accounts.pop(account_id_2)
        
        return True

    def get_balance(self, timestamp: int, account_id: str, time_at: int) -> int | None: 
        '''
        should return the total amount of money in the account account_id at the given timestamp time_at. If the specified 
        account did not exist at a given time time_at, returns None. 
        o If queries have been processed at timestamp time_at, get_balance must reflect the 
            account balance after the query has been processed. 
        o If the account was merged into another account, the merged account should inherit its 
            balance history.'''
        if account_id not in self.accounts.keys():
            if isinstance(account_id, Account):
                if account_id.creation_timestamp > time_at:
                    return None
            else: 
                greatest_time_at_key = None                
                for key in Account[account_id].balance_history.keys():
                    if key <= time_at:
                        if greatest_time_at_key is None or greatest_time_at_key <= key:
                            greatest_time_at_key = key
                print(greatest_time_at_key)
                
                return Account[account_id].balance_history[greatest_time_at_key]
            
        #if account DNE at time_at, return none
        if self.accounts[account_id].creation_timestamp > time_at:
            return None   
        else:
            #if pending queries exist at same time_at, process queries then get_balance
            self.process_cashbacks(time_at)
            #else return total_amount at time_at
            greatest_time_at_key = None                
            for key in self.accounts[account_id].balance_history.keys():
                if key <= time_at:
                    if greatest_time_at_key is None or greatest_time_at_key <= key:
                        greatest_time_at_key = key
            if greatest_time_at_key is None: 
                raise ValueError(f"nothing in {account_id} at {time_at}")
            print(greatest_time_at_key)
            
            return self.accounts[account_id].balance_history[greatest_time_at_key]
