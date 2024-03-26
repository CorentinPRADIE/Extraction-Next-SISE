from constantes import engagement_effect_id
import os
import pandas as pd

columns_order = ['operateur_id', 'action_id', 'projet_id', 'projet_nom', 'siren', 'beneficiaire_id', 'effect_id', 'period_id', 'montant']

def script_lignes_non_uniques(path_fact_enr_parquet, OPERATEURS, dim_projets, BASE_DIR):
    os.makedirs(BASE_DIR, exist_ok=True)
    use_cols = ['operateur_id', 'action_id', 'projet_id', 'beneficiaire_id', 'effect_id', 'period_id', 'montant', 'type_de_montant']
    for OPERATEUR in OPERATEURS:
        filters = [('operateur_id', '==', OPERATEUR), ('effect_id', 'in', engagement_effect_id), ('type_de_montant', '==', 'Financier Consommation'), ('montant', '!=', 0)]
        fact_enr = pd.read_parquet(path_fact_enr_parquet, columns=use_cols, filters=filters)
        fact_enr = pd.merge(fact_enr, dim_projets, on='projet_id', how='left')
        fact_enr['siren'] = fact_enr['beneficiaire_id'].str.split('_',n=1).str[-1].str[:9]
        fact_enr = fact_enr[columns_order]
        mask = fact_enr.duplicated(subset=['projet_id', 'period_id', 'siren', 'effect_id'], keep=False)
        
        fact_enr[mask].to_excel(os.path.join(BASE_DIR, f'{OPERATEUR}_Lignes_non_uniques.xlsx'), index=False)