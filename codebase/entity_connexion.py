
import warnings

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd

from attic import paths
from attic.connection_settings import connectionSettings
from attic.constants import EntityType
from eutl_orm import Account, AccountHolder
from eutl_orm import DataAccessLayer

dal = DataAccessLayer(**connectionSettings)
session = dal.session


def map_account_to_accountHolder() -> pd.DataFrame:
    """Return a mapping between account and account holder.

    Build a pandas DataFrame where each row corresponds to an Account. Each entry contains:
    - the account id (account_id)
    - the id of its account holder (accountHolder_id)
    - the name of its account holder name (accountHolder_name)

    The DataFrame is used to map each account with its account holder. Since this mapping is independent of a
    particular Entity under consideration, we define it at the module level. It is built when the module is imported.
    """
    mapping = pd.DataFrame(session.query(Account.id, Account.accountHolder_id).all())
    mapping.dropna(inplace=True)
    mapping["accountHolder_id"] = mapping["accountHolder_id"].astype(int)
    mapping.rename(columns={"id": "account_id"}, inplace=True)

    # add accountHolder name
    account_holder_id_to_name = pd.DataFrame(session.query(AccountHolder.id, AccountHolder.name).all())
    account_holder_id_to_name.rename(columns={"id": "accountHolder_id", "name": "accountHolder_name"}, inplace=True)
    mapping = mapping.merge(account_holder_id_to_name, on="accountHolder_id")

    return mapping


# map Account to AccountHolder (id and name)
account_to_accountHolder = map_account_to_accountHolder()


class EntityConnexion:
    """Class to build and plot the graph of transactions between a given Entity and the other comparable Entities."""

    sides = ['transferring', 'acquiring']

    def __init__(self, entity_type, entity_id, period=(None, None)):
        """Instantiate EntityConnexion object for the considered entity."""
        if entity_type not in [EntityType.Account, EntityType.AccountHolder, EntityType.Company]:
            raise ValueError(f'entity_type needs to be one of Account, Company or AccountHolder. '
                             f'Your input: {entity_type}')
        else:
            self.entity_type = entity_type

        self.entity_id = entity_id
        self.period = period
        self.accounts = None
        self.transactions = None
        self.entity_name = None

    def build_transaction_graph(self, keep_interactive_plot=False):
        """Build and plot the transaction graph for the considered entity."""
        self.get_accounts()
        self.get_transactions()
        self.plot_arrows(keep_interactive_plot)

    def get_accounts(self):
        """Look up and return the list of accounts related to the considered Entity."""

        if self.entity_type == EntityType.Account:
            self.accounts = session.query(Account).filter(Account.id == self.entity_id).all()
            self.entity_name = self.accounts[0].name
        elif self.entity_type == EntityType.AccountHolder:
            self.accounts = session.query(Account).filter(Account.accountHolder_id == self.entity_id).all()
            self.entity_name = session.query(AccountHolder).filter(AccountHolder.id == self.entity_id).first().name
        elif self.entity_type == EntityType.Company:
            self.accounts = session.query(Account).filter(Account.companyRegistrationNumber == self.entity_id).all()
            self.entity_name = ''

        print(f'You are investigating {self.entity_type} number {self.entity_id} '
              f'(name: {self.entity_name}) - {len(self.accounts)} related accounts')

    def get_transactions(self):
        """Get list of transaction involving each Account assigned to the Entity."""

        transaction_tables = []
        for account in self.accounts:
            transaction_tables.append(account.get_transactions())

        if len(transaction_tables) == 1 and transaction_tables[0] is None:
            return

        transactions = pd.concat(transaction_tables)
        transactions['datetime'] = pd.to_datetime(transactions.index)

        column_list = ['datetime', 'amount',
                       'transferringAccount_id', 'transferringAccountName', 'transferringAccountType',
                       'acquiringAccount_id', 'acquiringAccountName', 'acquiringAccountType',
                       'transactionTypeMain', 'transactionTypeSupplementary', 'unitType']

        if not set(column_list).issubset(transactions.columns):
            warnings.warn('At least one of the required columns is missing from the transaction table. Investigate!')
            return

        transactions = transactions[column_list].copy()

        if self.entity_type == 'Account':
            # rename AccountHolder -> Entity
            for side in ['transferring', 'acquiring']:
                transactions.rename(columns={f'{side}Account_id': f'{side}Entity_id',
                                             f'{side}AccountName': f'{side}Entity_name',
                                             f'{side}AccountType': f'{side}Entity_type'}, inplace=True)
        elif self.entity_type == 'AccountHolder':
            # map each (transferring or acquiring) account to its account holder
            for side in self.sides:
                account_to_accountHolder_temp = account_to_accountHolder.copy()
                account_to_accountHolder_temp.rename(columns={"account_id": f"{side}Account_id",
                                                              "accountHolder_id": f"{side}AccountHolder_id",
                                                              "accountHolder_name": f"{side}AccountHolder_name"}, inplace=True)
                transactions = transactions.merge(account_to_accountHolder_temp)

            # discard internal transactions between accounts of the same account holder
            transactions = transactions[transactions["acquiringAccountHolder_id"] != transactions["transferringAccountHolder_id"]]

            # drop information relative to the accounts (we are only interested in account holders)
            transactions.drop(columns=["acquiringAccount_id", "acquiringAccountName", "acquiringAccountType",
                                       "transferringAccount_id", "transferringAccountName", "acquiringAccountType"],
                              inplace=True)

            # rename AccountHolder -> Entity
            for side in self.sides:
                transactions.rename(columns={f'{side}AccountHolder_id': f'{side}Entity_id',
                                             f'{side}AccountHolder_name': f'{side}Entity_name'}, inplace=True)

        elif self.entity_type == 'Company':
            # todo: merge with account get company_id > merger with accountHolder (transferring and acquiring)
            #  then rename columns
            pass

        # todo: add an option to remove "admin" transactions

        self.transactions = transactions

        print('  > Found {} transactions\n'.format(len(self.transactions)))
        # todo: add the period condition here

    def plot_arrows(self, keep_interactive_plot=False):
        """Plot transaction graph and save it as png file."""

        # todo: add selected dates on graph

        if self.transactions is None:
            return

        transaction_table = self.transactions
        this_node = self.entity_id

        # Sometimes there are missing values in the transaction dataframe
        for side in {'transferring', 'acquiring'}:
            if transaction_table[f'{side}Entity_id'].isnull().any():
                warnings.warn(f'  Some {side}Entity IDs missing ... replacing by -1/unknown')
                fillval = {'transferringEntity_id': -1,
                           'transferringEntity_name': 'unknown', 'transferringEntity_type': 'unknown'}
                transaction_table.fillna(value=fillval, inplace=True)

        # Make graph from transactions dataframe
        transaction_graph = nx.from_pandas_edgelist(transaction_table, source='transferringEntity_id', target='acquiringEntity_id',
                                                    edge_attr='amount', create_using=nx.DiGraph())
        if len(transaction_graph) > 40:  # if too many nodes, no point in plotting
            warnings.warn('Too many nodes, not producing graph')
            warnings.warn('Too many nodes, not producing graph')
            return

        # determine receiver/sender/trader status
        attrs = {}
        trans_entities = set(transaction_table['transferringEntity_id'])
        acqui_entities = set(transaction_table['acquiringEntity_id'])

        # colors
        color_legend = {'this': 'green', 'trader': 'violet', 'sender': 'blue', 'receiver': 'red'}
        color_handles = []
        for c in color_legend:
            color_handles.append(mpatches.Patch(color=color_legend[c], label=c))

        entity_id_to_name = self.map_entity_id_to_name(transaction_table)

        # loop over nodes
        for node in transaction_graph:
            attrs[node] = {}
            # todo: change color based on account type not on trader_type
            attrs[node]['name'] = entity_id_to_name[node]
            attrs[node]['id'] = node
            if (node in trans_entities) and (node in acqui_entities):
                attrs[node]['trader_type'] = 'trader'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
                # TODO: commented out the line below because there is no "Type" column in the AccountHolder table.
                #  -> is there such a thing as a "type" attribute for AccountHolders of Companies ?
                # attrs[node]['type'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_type']
            elif node in trans_entities:
                attrs[node]['trader_type'] = 'sender'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
                # TODO: commented out the line below because there is no "Type" column in the AccountHolder table.
                #  -> is there such a thing as a "type" attribute for AccountHolders of Companies ?
                # attrs[node]['type'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_type']
            elif node in acqui_entities:
                attrs[node]['trader_type'] = 'receiver'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
                # TODO: commented out the line below because there is no "Type" column in the AccountHolder table.
                #  -> is there such a thing as a "type" attribute for AccountHolders of Companies ?
                # attrs[node]['type'] = df[df['acquiringEntity_id'] == node].iloc[0]['acquiringEntity_type']
            if node == this_node:  # just changing the trader type and color (name and type should have been set before)
                attrs[node]['trader_type'] = 'this'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]

        nx.set_node_attributes(transaction_graph, attrs)

        # defining width of arrows
        width_thinnest_edge = 0.05
        width_thickest_edge = 3
        max_width = max([transaction_graph[u][v]['amount'] for u, v in transaction_graph.edges])
        width = [width_thickest_edge * transaction_graph[u][v]['amount'] / max_width + width_thinnest_edge for u, v in transaction_graph.edges()]

        # get list of nodes and reorder based on trader type
        list_of_nodes = [x for _, x in sorted(zip([attrs[n]['trader_type'] for n in transaction_graph], transaction_graph))]
        list_of_nodes.remove(this_node)

        # define circular position
        pos = nx.circular_layout(list_of_nodes, scale=2)
        pos[this_node] = np.array([0, 0])

        # define label positions (slightly below node)
        pos_attrs = {}
        for node, coords in pos.items():
            pos_attrs[node] = (coords[0], coords[1] - .25)

        # plot the whole thing
        fig, ax = plt.subplots(figsize=(10, 7))
        nx.draw(transaction_graph, pos=pos,
                connectionstyle='arc3,rad=0.1', node_color=[attrs[x]['color'] for x in attrs],
                with_labels=False, width=width)
        nx.draw_networkx_labels(transaction_graph, pos_attrs, labels={n: f"{attrs[n]['name']} \n({attrs[n]['id']})" for n in attrs})
        ax.legend(handles=color_handles)
        if self.entity_type == 'Company':
            plt.title(f'ETS trading connections for {self.entity_type}: {self.entity_id}')
        else:
            plt.title(f'ETS trading connections for {self.entity_type}: {self.entity_name}')
        plt.tight_layout()

        full_path_plot = paths.path_plots / f'arrows_{self.entity_type}_{self.entity_id}.png'
        plt.savefig(full_path_plot, dpi=500)
        print(f"Transaction graph plot saved under: {full_path_plot}")

        if not keep_interactive_plot:
            plt.close()

    def map_entity_id_to_name(self, transaction_table: pd.DataFrame) -> pd.Series:
        """Map the name of an entity to its name, both from the transaction table.

        The mapping is returned as a pandas Series with index entity id and value entity name.
        # TODO: note that if the entity type is AccountHolder, we already have such a mapping in account_to_accountHolder
        """
        mapping = []
        for side in self.sides:
            mapping_side = transaction_table[[f"{side}Entity_id", f"{side}Entity_name"]].copy()
            mapping_side.rename(columns={f"{side}Entity_id": "entity_id",
                                         f"{side}Entity_name": "entity_name"}, inplace=True)
            mapping.append(mapping_side)
        mapping = pd.concat(mapping).drop_duplicates()
        mapping = mapping.set_index("entity_id").squeeze().sort_index()
        return mapping

    def plot_cumul(self):
        # todo: add the plot of cumulative holdings
        return

    def plot_compliance(self):
        # todo: add the plot of emissions, free allocations and surrendered certificates
        return
