"""
This example script demonstrates how to build and plot the transaction graph for an account holder
(here: Wien Energie GmbH).
"""
from codebase.entity_connexion import EntityConnexion
from attic.constants import EntityType

entity_id = 81  # Wien Energie GmbH, see: https://euets.info/holder/81

# instantiate EntityConnexion object
connector = EntityConnexion(entity_type=EntityType.AccountHolder, entity_id=entity_id)

# prepare transaction table and plot/save transaction graph
connector.build_transaction_graph(keep_interactive_plot=True)

