import time
from banking_system import BankingSystem

class Account:
    def __init__(self, id, balance=0, total_outgoing=0): 
        self.id = id
        self.balance = balance
        self.total_outgoing = total_outgoing

    # add amount if transferred to account
    def deposit(self, amount):
        self.balance += amount
        return self.balance

    # decrease amount if withdrawn from account
    def withdraw(self, amount): 
        self.balance -= amount
        self.total_outgoing += amount
        return self.balance    

class BankingSystemImpl(BankingSystem):
    def __init__(self): 
        self.accounts = {}        

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
            balance = self.accounts[account_id].deposit(amount)
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
        self.accounts[target_account_id].deposit(amount)
        source_balance = self.accounts[source_account_id].withdraw(amount)
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
        
    