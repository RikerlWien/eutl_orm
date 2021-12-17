
import pandas as pd

from eutl_orm import DataAccessLayer
from attic.connection_settings import connectionSettings
from eutl_orm import Installation, Account, AccountHolder

dal = DataAccessLayer(**connectionSettings)
session = dal.session


class EntityConnexion():
    def __init__(self, entity_type, entity_id, period=(None, None)):
        if entity_type in ['Account', 'Company', 'AccountHolder']:
            self.entity_type = entity_type
        else:
            print('entity_type needs to be one of Account, Company or AccountHolder')
            raise ValueError
        self.entity_id = entity_id
        self.period = period
        self.accounts = None
        self.transactions = None
        self.name = None

        self.get_accounts()
        self.get_transactions()

    def get_accounts(self):
        # look up list of accounts and return it
        if self.entity_type == 'Account':
            self.accounts = session.query(Account).filter(Account.id == self.entity_id).all()
            self.name = self.accounts[0].name
        elif self.entity_type == 'AccountHolder':
            self.accounts = session.query(Account).filter(Account.accountHolder_id == self.entity_id).all()
            self.name = session.query(AccountHolder).filter(AccountHolder.id == self.entity_id).first().name
        elif self.entity_type == 'Company':
            self.accounts = session.query(Account).filter(Account.companyRegistrationNumber == self.entity_id).all()
            self.name = ''

        print('You are working with {} with id {} ({}) - {} related accounts'.format(self.entity_type,
                                                                                     self.entity_id,
                                                                                     self.name,
                                                                                     len(self.accounts)))

    def get_transactions(self):
        transaction_tables = []
        for account in self.accounts:
            transaction_tables.append(account.get_transactions())

        transactions = pd.concat(transaction_tables)

        # add date and datetime information to table (to price transactions with daily ETS price)
        transactions['date'] = pd.to_datetime(transactions.index.date)
        transactions['datetime'] = pd.to_datetime(transactions.index)

        # aggregate transaction blocks into individual transactions
        self.transactions = transactions.groupby('transactionID').agg({'amount': 'sum',
                                                                       'amount_directed': 'sum',
                                                                       'transferringAccount_id': 'first',
                                                                       'acquiringAccount_id': 'first',
                                                                       'date': 'first',
                                                                       'datetime': 'first',
                                                                       'acquiringAccountName': 'first',
                                                                       'transferringAccountName': 'first',
                                                                       'transactionTypeMain': 'first',
                                                                       'transactionTypeSupplementary_id': 'first'})

        print(self.transactions.dtypes)

        print('  > Found {} transactions'.format(len(self.transactions)))
        print()
        # todo: add the period condition here



EntityConnexion('Account', 133)
EntityConnexion('AccountHolder', 145)
EntityConnexion('Company', 'FN 71396 w')
