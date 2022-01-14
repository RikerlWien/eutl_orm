"""
This example script demonstrates how to build and plot the transaction graph for an (operator holding) account
(here: the Spittelau waste incineration plant in Vienna, AT).
"""
from codebase.entity_connexion import EntityConnexion
from attic.constants import EntityType

entity_id = 493  # Spittelau waste incinerator, see: https://euets.info/account/493

# instantiate EntityConnexion object
connector = EntityConnexion(entity_type=EntityType.Account, entity_id=entity_id)

# prepare transaction table and plot/save transaction graph
connector.build_transaction_graph(keep_interactive_plot=True)

