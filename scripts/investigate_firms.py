import datetime as dt
from sqlalchemy import or_
import matplotlib.pyplot as plt
import pandas as pd

import eutl_orm.model as mo
from eutl_orm import DataAccessLayer
from attic.connection_settings import connectionSettings
from attic import paths
from attic.constants import EntityType

dal = DataAccessLayer(**connectionSettings)
session = dal.session

"""
This script computes and plots a number of descriptive statistics:
> nb_accounts per year (total and split up by accountType)
> nb_accountHolders per year 
> nb_companies per year
At the very end it investigates where companies do not share an accountHolder_id and company_id
"""

# ------------------------------------------------------------
# Controls and constants
# ------------------------------------------------------------

do_data = False
do_plotting = False
year_range = range(2012, 2022)


# ------------------------------------------------------------
# Collect the data
# ------------------------------------------------------------

# mapping between account type id and account type (e.g.: 120-0 <-> Former Operator Holding Account)
account_type_table = pd.read_sql(session.query(mo.AccountType).statement, con=session.bind)
account_type_table.rename(columns={'id': 'accountType_id'}, inplace=True)

if do_data:
    # prepare data
    df_comp_or_accHold_id_not_matching = pd.DataFrame()
    nb_accounts_per_account_type_per_year = pd.DataFrame()

    for year in year_range:

        # query accounts which were active during considered year
        account_query = session.query(mo.Account)\
            .filter(mo.Account.openingDate <= dt.date(year, 12, 31))\
            .filter(or_(mo.Account.closingDate >= dt.date(year, 1, 1),
                        mo.Account.isOpen))

        account_df = pd.read_sql(sql=account_query.statement,
                                 con=session.bind)

        n_account_holders = account_df.groupby('accountHolder_id').size().reset_index(name='counts')
        n_companies = account_df.groupby('companyRegistrationNumber').size().reset_index(name='counts')

        # check for accounts without account holder/company registration number
        missing_account_holders = account_df['accountHolder_id'].isna().sum()
        missing_company_registration_nbs = account_df['companyRegistrationNumber'].isna().sum()
        print(f'Missing values in {year}:')
        print(f' > {missing_account_holders} accounts without accountHolder')
        print(f' > {missing_company_registration_nbs} accounts without company registration number')

        account_types = account_df.groupby('accountType_id').size().reset_index(name='counts')
        account_types = account_types.merge(account_type_table, on='accountType_id', how='left')

        nb_accounts_per_account_type_per_year = nb_accounts_per_account_type_per_year.append(pd.DataFrame([[year] + account_types['counts'].tolist()],
                                                                                                          columns=['year'] + account_types['description'].tolist()))

        df_comp_or_accHold_id_not_matching = df_comp_or_accHold_id_not_matching.append(pd.DataFrame([[year, len(account_query.all()), len(n_account_holders), len(n_companies)]],
                                                                                                    columns=['year', 'nb_accounts', 'nb_accountHolders', 'nb_companies']))

    df_comp_or_accHold_id_not_matching.to_excel(paths.path_data / 'descriptive_stats_per_year.xlsx', index=False)
    nb_accounts_per_account_type_per_year.fillna(value=0, inplace=True)
    nb_accounts_per_account_type_per_year.to_excel(paths.path_data / 'descriptive_stats_per_year_accountTypes.xlsx', index=False)

else:
    df_comp_or_accHold_id_not_matching = pd.read_excel(paths.path_data / 'descriptive_stats_per_year.xlsx')
    nb_accounts_per_account_type_per_year = pd.read_excel(paths.path_data / 'descriptive_stats_per_year_accountTypes.xlsx')

# ------------------------------------------------------------
# Do the plotting
# ------------------------------------------------------------

if do_plotting:

    # Plot evolution of number of accounts of each entity type over time
    for entity_type in ['accounts', 'accountHolders', 'companies']:
        fig, ax = plt.subplots(figsize=(10, 7))
        x = df_comp_or_accHold_id_not_matching['year']
        y = df_comp_or_accHold_id_not_matching[f'nb_{entity_type}']
        plt.bar(x, y, color='navy')
        plt.title(f'Number of active {entity_type}')
        ax.xaxis.set_ticks(year_range)
        plt.tight_layout()
        plt.savefig(paths.path_plots / f'descriptive/nb_{entity_type}.png', dpi=500)
        plt.close()

    # Select most relevant account types, i.e. those for which there has been on average >300 active accounts/year
    dfa_summary = pd.DataFrame(nb_accounts_per_account_type_per_year.mean().reset_index())
    dfa_summary.rename(columns={'index': 'accountType', 0: 'count'}, inplace=True)
    dfa_summary = dfa_summary[dfa_summary['accountType'] != 'year']
    dfa_summary.sort_values(by=['count'], ascending=False, inplace=True)
    dfa_summary['plot'] = 0
    dfa_summary.loc[dfa_summary['count'] > 300, 'plot'] = 1
    account_types_to_be_plotted = dfa_summary[dfa_summary['plot'] == 1]['accountType'].tolist()

    col_list = list(nb_accounts_per_account_type_per_year)
    for a in account_types_to_be_plotted:
        col_list.remove(a)
    nb_accounts_per_account_type_per_year['other'] = nb_accounts_per_account_type_per_year[col_list].sum(axis=1)
    account_types_to_be_plotted = account_types_to_be_plotted + ['other']

    dft = nb_accounts_per_account_type_per_year[['year'] + account_types_to_be_plotted].copy()
    dft.to_excel(paths.path_data / 'descriptive_stats_per_year_accountTypes_PLOT.xlsx')

    # Stacked plot by
    cumval=0
    fig = plt.figure(figsize=(10, 7))
    for col in account_types_to_be_plotted:
        plt.bar(nb_accounts_per_account_type_per_year['year'], nb_accounts_per_account_type_per_year[col], bottom=cumval, label=col)
        cumval = cumval + nb_accounts_per_account_type_per_year[col]
    _ = plt.xticks(rotation=30)
    _ = plt.legend(fontsize=10)
    ax.set_ylabel("number of active accounts")
    ax.set_title("number of active account over time")
    plt.savefig(paths.path_plots / f'descriptive/nb_accountType.png', dpi=500)
    plt.close()

    # Plot number by account type
    if False:
        for entity_type in nb_accounts_per_account_type_per_year.columns:
            if entity_type != 'year':
                fig, ax = plt.subplots(figsize=(10, 7))
                x = nb_accounts_per_account_type_per_year['year']
                y = nb_accounts_per_account_type_per_year[entity_type]
                plt.bar(x, y, color='navy')
                plt.title(f'Number of active {entity_type}')
                ax.xaxis.set_ticks(year_range)
                plt.tight_layout()
                plt.savefig(paths.path_plots / f'descriptive/account_types/nb_{entity_type}.png', dpi=500)
                plt.close()

# ----------------------------------------------------------------------
# Understand where accountHolder_id and company_id don't match
# ----------------------------------------------------------------------

# query all accounts active in 2020 (example year)
year_studied = 2020
account_query = session.query(mo.Account)\
    .filter(mo.Account.openingDate <= dt.date(year_studied, 12, 31))\
    .filter(or_(mo.Account.closingDate >= dt.date(year_studied, 1, 1),
                mo.Account.isOpen))
account_df = pd.read_sql(sql=account_query.statement, con=session.bind)\
    .fillna(value={'companyRegistrationNumber': 'NA'})

# get accountHolder_ids and corresponding names
accountHolder_mapping_query = session.query(mo.AccountHolder.id, mo.AccountHolder.name)
accountHolder_mapping = pd.read_sql(accountHolder_mapping_query.statement, con=session.bind)
accountHolder_mapping.rename(columns={'id': 'accountHolder_id',
                                      'name': 'accountHolder_name'}, inplace=True)

# FIRST QUESTION: what accounts do not have a company_registration_number?
# select accounts with missing compRegNum
missing_company_id_list = ['NA', 'na', '0', '-', 'n.a. Körperschaft des öffentl. Rechts', 'Government',
                           'Co-operative Society', 'Academic', 'Not applicable', 'TBA',
                           'Körperschaft des öffentl. Rechts']
account_df.loc[account_df.companyRegistrationNumber.isin(missing_company_id_list), 'companyRegistrationNumber'] = 'NA'
missing_companies = account_df[account_df.companyRegistrationNumber.isin(missing_company_id_list)]
# add the information on the accountType
missing_companies = missing_companies.merge(account_type_table, on='accountType_id', how='left', indicator=True)
if len(missing_companies[missing_companies._merge != 'both']) > 0:
    ValueError('Some not merged')
missing_companies.drop(labels='_merge', axis=1, inplace=True)
# add the information on the accountHolder_name
missing_companies = missing_companies.merge(accountHolder_mapping, on='accountHolder_id', how='left', indicator=True)
if len(missing_companies[missing_companies._merge != 'both']) > 0:
    ValueError('Some not merged')
missing_companies.drop(labels='_merge', axis=1, inplace=True)
# Output
print(f'> In {year_studied}, out of {len(account_df)} active accounts, {len(missing_companies)} had no company_id.')
print(f'  An analysis by hand for 2020 shows that 70% of those are government accounts and many other private accounts.')
missing_companies.to_excel(paths.path_data / f'missing_companies_{year_studied}.xlsx')

# Group accounts by account holder id AND company registration number
df_comp_or_accHold_id_not_matching = account_df.groupby(['accountHolder_id', 'companyRegistrationNumber'], as_index=False)\
    ['id'].count()
df_comp_or_accHold_id_not_matching.rename(columns={'id': 'nb_accounts'}, inplace=True)
df_comp_or_accHold_id_not_matching['nb_CR_for_this_AH'] = df_comp_or_accHold_id_not_matching.groupby('accountHolder_id')['accountHolder_id'].transform('count')
df_comp_or_accHold_id_not_matching['nb_AH_for_this_CR'] = df_comp_or_accHold_id_not_matching.groupby('companyRegistrationNumber')['companyRegistrationNumber'].transform('count')
df_comp_or_accHold_id_not_matching = df_comp_or_accHold_id_not_matching[(df_comp_or_accHold_id_not_matching.nb_AH_for_this_CR > 1) |
                                                                        (df_comp_or_accHold_id_not_matching.nb_CR_for_this_AH > 1)]
df_comp_or_accHold_id_not_matching = df_comp_or_accHold_id_not_matching.merge(accountHolder_mapping, on='accountHolder_id', how='left', indicator=True)
if len(df_comp_or_accHold_id_not_matching[df_comp_or_accHold_id_not_matching._merge != 'both']) > 0:
    ValueError('Some not merged')
df_comp_or_accHold_id_not_matching.drop(labels='_merge', axis=1, inplace=True)
df_comp_or_accHold_id_not_matching = df_comp_or_accHold_id_not_matching[['accountHolder_id', 'accountHolder_name',
                                                                         'companyRegistrationNumber', 'nb_accounts',
                                                                         'nb_AH_for_this_CR', 'nb_CR_for_this_AH']]
df_comp_or_accHold_id_not_matching.to_excel(paths.path_data / f'accountHolder_company_weird_{year_studied}.xlsx')
