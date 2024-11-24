from banking_system import BankingSystem
import heapq
import math

class Account:
    def __init__(self, timestamp, id, balance=0, total_outgoing=0): 
        # timestamp for when account was created
        self.creation_timestamp = timestamp
        # account ID
        self.id = id
        # current balance for account
        self.balance = balance
        # account balances for past timestamps
        self.balance_history = {timestamp: balance}
        # total amount withdrawn from account
        self.total_outgoing = total_outgoing
        
    # add amount if transferred or deposited to account, including account merges
    def deposit(self, timestamp: int, amount: int):
        # increments account balance by deposited amount
        self.balance += amount
        # adds timestamp with balance to record account balance change
        self.balance_history[timestamp] = self.balance
        return self.balance

    # decrease amount if withdrawn from account
    def withdraw(self, timestamp: int, amount: int): 
        # decrements account balance by withdrawn amount
        self.balance -= amount
        # adds timestamp with balance to record account balance change
        self.balance_history[timestamp] = self.balance
        # increments total outgoing by withdrawn amount
        self.total_outgoing += amount
        return self.balance     

class BankingSystemImpl(BankingSystem):
    def __init__(self): 
        # dictionary of valid accounts in banking system
        self.accounts = {}
        # dictionary of invalid accounts that have been merged with other valid accounts
        # values are stored in (account, merge_timestamp) tuples
        self.merged_accounts = {}
        # counter of number of withdrawals to generate payment IDs
        self.num_withdraws = 1
        # priority queue to keep track of cashbacks that have not been processed (pending)
        self.pending_cashbacks = [] 
        # nested list to keep track of processed cashbacks
        self.completed_cashbacks = []

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
            # process cashbacks at or before timestamp before calculating balance
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
        # process cashbacks before depositing, withdrawing, and reporting balance
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
        # checks that account_id is valid (in self.accounts)           
        if account_id not in self.accounts.keys():
            return None
                
        # process cashbacks before evaluating balance and making payment
        self.process_cashbacks(timestamp)

        # make sure that account has enough balance to make payment
        if self.accounts[account_id].balance < amount:
            return None
        
        # withdraw amount from account from which payment is being made
        self.accounts[account_id].withdraw(timestamp, amount)
        # generate payment ID from total number of withdrawals
        payment_id = f"payment{self.num_withdraws}"

        # 2% cashback needs to be deposited to account after payment, we add future cashbacks to pending_cashbacks priority queue
        # push (timestamp + 24 hrs, account_id, payment_id, cashback amount) to pending_cashbacks
        heapq.heappush(self.pending_cashbacks, (timestamp + 86400000, account_id, payment_id, math.floor(amount*0.02)))
        
        # increment total number of withdrawals after payment
        self.num_withdraws += 1
        return payment_id

    def process_cashbacks(self, timestamp: int):
        # check whether first timestamp in priority queue is before current timestamp
        while self.pending_cashbacks and self.pending_cashbacks[0][0] <= timestamp:
            # deposit the cashback and take the cashback off of the pending_cashbacks priority queue
            cashback_time, cashback_account_id, payment_id, cashback_amount = heapq.heappop(self.pending_cashbacks)
            self.accounts[cashback_account_id].deposit(cashback_time, cashback_amount)   
            # append the processed cashback to completed_cashbacks
            self.completed_cashbacks.append([cashback_time, cashback_account_id, payment_id, cashback_amount])
        
    def get_payment_status(self, timestamp: int, account_id: str, payment: str) -> str | None:
        # check whether account_id exists, return None if not
        if account_id not in self.accounts.keys():
            return None
        # process pending cashbacks before evaluating cashback payment status
        self.process_cashbacks(timestamp)

        # iterate over pending cashbacks to find if there is a matching account/payment id for cashbacks in progress        
        for cashback in self.pending_cashbacks: 
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback 
            if account_id == cashback_account_id and payment == cashback_payment_id:
                if cashback_time > timestamp:
                    return "IN_PROGRESS"
        
        # iterate over completed cashbacks to find if there is a matching account/payment id for cashbacks received
        for cashback in self.completed_cashbacks:
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback
            if account_id == cashback_account_id and payment == cashback_payment_id:
                return "CASHBACK_RECEIVED"
            
        # return None if given payment id does not exist for specified account
        # return None if payment transaction was for a different account_id
        return None
    
    def merge_cashbacks(self, timestamp: int, account_id_1: str, account_id_2: str):

        # after merging accounts (merge acct2 with acct1), all pending cashbacks for acct2 need to be paid to acct1
        # all cashbacks for acct2 in pending_cashbacks is replaced with acct1
        for i, cashback in enumerate(self.pending_cashbacks): 
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback 
            if cashback_account_id == account_id_2:
                updated_cashback = (cashback_time, account_id_1, cashback_payment_id, cashback_amount)
                self.pending_cashbacks[i] = updated_cashback
                heapq.heapify(self.pending_cashbacks)

        # all completed cashbacks for acct2 need to be marked as paid to acct1 for future status calculations
        # all cashbacks for acct2 in completed_cashbacks is replaced with acct1
        for i, cashback in enumerate(self.completed_cashbacks): 
            cashback_time, cashback_account_id, cashback_payment_id, cashback_amount = cashback 
            if cashback_account_id == account_id_2:
                updated_cashback = (cashback_time, account_id_1, cashback_payment_id, cashback_amount)
                self.completed_cashbacks[i] = updated_cashback
                heapq.heapify(self.completed_cashbacks)            
        
    
    def merge_accounts(self, timestamp: int, account_id_1: str, account_id_2: str) -> bool:
        
        # checks that accounts being merged are unique
        if account_id_1 == account_id_2:
            return False
        # checks that both accounts are valid (in self.accounts)
        if (account_id_1 not in self.accounts.keys()) or (account_id_2 not in self.accounts.keys()):
            return False
        
        # update balance of acct1 to include acct2 balance
        # calling the deposit function updates self.balance and self.balance_history
        self.accounts[account_id_1].deposit(timestamp, self.accounts[account_id_2].balance)        
        # update total outgoing of acct1 to include acct2 total outgoing
        self.accounts[account_id_1].total_outgoing += self.accounts[account_id_2].total_outgoing
                
        # call to function for updating pending_cashbacks and completed_cashbacks to replace acct2 with acct1
        self.merge_cashbacks(timestamp, account_id_1, account_id_2)
        # removing acct2 from being a valid account ID, removing acct2 from self.accounts
        self.merged_accounts[account_id_2] = (self.accounts.pop(account_id_2), timestamp)       
        
        # merging accounts was successful
        return True

    def get_balance(self, timestamp: int, account_id: str, time_at: int) -> int | None: 

        # we can get balance of account IDs that are currently valid or have been merged
        # checking whether account_id is currently valid
        if account_id in self.accounts.keys():
            account = self.accounts[account_id]
        # checking whether account_id has already been merged
        elif account_id in self.merged_accounts.keys():
            account, merge_timestamp = self.merged_accounts[account_id]
            # check whether account has been merged before time_at
            # function returns None if merged before time_at, because account is not valid at time_at
            if merge_timestamp <= time_at:
                return None
        # if account_id has never been valid, return None            
        else: 
            return None
        
        # check that account is createad before time_at so that it is valid at time_at
        if account.creation_timestamp > time_at:
            return None      
        # given that account is valid at time_at, process cashbacks and get balance for account at time_at
        else:
            # if pending queries exist at same time_at, process queries then get_balance
            self.process_cashbacks(time_at)
            # if time_at is not a valid key in balance_history, it means that there was not change in balance at time_at
            # we calculate the greatest timestamp key that is less than time_at if time_at is not a valid key in balance_history
            greatest_time_at_key = None                
            for key in account.balance_history.keys():
                if key <= time_at:
                    if greatest_time_at_key is None or greatest_time_at_key <= key:
                        greatest_time_at_key = key
            
            # return the balance at calculated timestamp
            return account.balance_history[greatest_time_at_key]
