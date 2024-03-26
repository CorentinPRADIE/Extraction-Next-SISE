import pandas as pd 
import os



def script_ch_benef_operateur(path_fact_enr, OPERATEUR, dim_projets, BASE_DIR):
    os.makedirs(BASE_DIR, exist_ok=True)

    cols = [
     'operateur_id',
     'action_id',
     'projet_id',
     'beneficiaire_id',
     'effect_id',
     'period_id',
     'montant',
     'type_de_montant']

    valid_effect_id = ['Engagement Avances remboursables',
                     'Engagement DNC',
                     'Engagement Fonds de Garantie',
                     'Engagement Intérêts DNC',
                     'Engagement Prêts',
                     'Engagement Prises de participation',
                     'Engagement Subventions',
                     'Engagement Dotations Décennales']

    filters = [
        ('operateur_id', '==', OPERATEUR),
        ('type_de_montant', '==', 'Financier Consommation'),
        ('effect_id', 'in', valid_effect_id)
    ]
    
    fact_enr = pd.read_parquet(path_fact_enr, filters=filters, columns=cols)
    fact_enr['beneficiaire_id'] = fact_enr['beneficiaire_id'].str.split('_').str[1]
    fact_enr['siren_id'] = fact_enr['beneficiaire_id'].str[:9]
    
    fact_enr.set_index(['projet_id', 'period_id'], inplace=True)
    fact_enr.sort_index(inplace=True)
    
    num_projet = fact_enr.index.get_level_values(0).nunique()

    def get_operateur_and_action_from_projet_id(projet_id, group):
        first_row = group[group.index.get_level_values(0) == projet_id].iloc[0]
        return first_row['operateur_id'], first_row['action_id'] 
    
    def get_amount(group_period, beneficiare_id):
        if isinstance(group_period['beneficiaire_id'], str):
            return group_period['montant'].sum()
        return group_period [ group_period['beneficiaire_id'] == beneficiare_id]['montant'].sum()
    
    def is_ch_siret_meme_siren(disappeared_beneficiaire_id : str, appeared_beneficiares : list[str], current_period, next_period):
        disappeared_siren = disappeared_beneficiaire_id[:9]
        
        for appeared_beneficiare_id in appeared_beneficiares:
            appeared_siren = appeared_beneficiare_id[:9]
            
            if appeared_siren == disappeared_siren:
                amount_disappeard = get_amount(current_period, disappeared_beneficiaire_id)
                amount_appeared = get_amount(next_period, appeared_beneficiare_id)
                
                if abs(amount_disappeard - amount_appeared) == 0:
                    return appeared_beneficiare_id
        return False
        
    disappeared_beneficiaires = []

    for projet_id, group in fact_enr.groupby(level=0):
        
        # Sort periods
        periods = sorted(set(group.index.get_level_values(1)))
    
        # Loop through each period_id
        for i in range(len(periods) - 1):
    
            # Current and next period rows
            current_period = group.loc[(projet_id, periods[i])]
            next_period = group.loc[(projet_id, periods[i + 1])]
            
            # Current and next peiod beneficiaire_id
            current_period_beneficiaires = None
            next_period_beneficiaires = None
            
            if isinstance(current_period['beneficiaire_id'], str):
                current_period_beneficiaires = {current_period['beneficiaire_id']}
            else:
                current_period_beneficiaires = set(current_period['beneficiaire_id'].values)

            if isinstance(next_period['beneficiaire_id'], str):
                next_period_beneficiaires = {next_period['beneficiaire_id']}
            else:
                next_period_beneficiaires = set(next_period['beneficiaire_id'].values)
            
          
            # Disappeared beneficiare_id between current and next period 
            disappeared_beneficiares = (current_period_beneficiaires - next_period_beneficiaires)
            
            # Loop through each disappeared beneficiaire_id
            for disappeared_beneficiaire_id in disappeared_beneficiares:
                amount_disappeared_beneficiare_id = get_amount(current_period, disappeared_beneficiaire_id)
                if amount_disappeared_beneficiare_id != 0:
    
                    # Appeared beneficiaire_id between current and next_period
                    appeared_beneficiares = next_period_beneficiaires - current_period_beneficiaires
                    
                    same_siren = is_ch_siret_meme_siren(disappeared_beneficiaire_id, appeared_beneficiares, current_period, next_period)
                    
                    operateur_id, action_id = get_operateur_and_action_from_projet_id(projet_id, group)
                    projet_nom = dim_projets.loc[dim_projets['projet_id'] == projet_id, 'projet_nom'].values[0]
                    disappeared_beneficiaires.append((operateur_id, action_id, projet_id, projet_nom, periods[i+1], disappeared_beneficiaire_id, amount_disappeared_beneficiare_id, same_siren))
    
    # Create a DataFrame to display disappeared beneficiaire_ids
    disappeared_df = pd.DataFrame(disappeared_beneficiaires, columns=['operateur_id', 'action_id', 'projet_id', 'projet_nom', 'next_period_id', 'disappeared_beneficiaire_id', 'amount_disappeared', 'same_siren'])
    
    disappeared_siren_diff = disappeared_df [ disappeared_df['same_siren'] == False]
    
    new_df = pd.DataFrame()
    new_df['NOM_PROJET'] = disappeared_siren_diff['projet_nom']
    new_df['ACTION'] = disappeared_siren_diff['action_id']
    new_df['OPERATEUR'] = disappeared_siren_diff['operateur_id']
    new_df['MOIS'] = disappeared_siren_diff['next_period_id'].str[4:6]  # Extract month from period_id
    
    
    new_df['ANNEE'] = disappeared_siren_diff['next_period_id'].str[:4]  # Extract year from period_id
    new_df['ID PROJET'] = disappeared_siren_diff['projet_id'].str[4:].str.upper()
    new_df['SIRET BENEFICIAIRE'] = disappeared_siren_diff['disappeared_beneficiaire_id']
    new_df['CHEF DE FILE DU PROJET'] = 'NON'
    
    extra_columns = [
        'ENGAGEMENT SUBVENTIONS', 'ENGAGEMENT DOTATION DECENNALES', 'ENGAGEMENT AVANCES REMBOURSABLES',
        'ENGAGEMENT FONDS DE GARANTIE', 'ENGAGEMENT PRETS', 'ENGAGEMENT PRISES DE PARTICIPATION',
        'ENGAGEMENT DNC', 'ENGAGEMENT INTERETS DNC', 'CONTRACTUALISATION SUBVENTIONS',
        'CONTRACTUALISATION DOTATIONS DECENNALES', 'CONTRACTUALISATION AVANCES REMBOURSABLES',
        'CONTRACTUALISATION FONDS DE GARANTIE', 'CONTRACTUALISATION PRETS',
        'CONTRACTUALISATION PRISES DE PARTICIPATION', 'CONTRACTUALISATION DNC',
        'CONTRACTUALISATION INTERETS DNC', 'DECAISSEMENT SUBVENTIONS',
        'DECAISSEMENT DOTATIONS DECENNALES', 'DECAISSEMENT AVANCES REMBOURSABLES',
        'DECAISSEMENT FONDS DE GARANTIE', 'DECAISSEMENT PRETS', 'DECAISSEMENT PRISES DE PARTICIPATION',
        'DECAISSEMENT DNC', 'DECAISSEMENT INTERETS DNC'
    ]
    
    for col in extra_columns:
        new_df[col] = 0
    
    
    with pd.ExcelWriter(f'{BASE_DIR}/Changements_de_SIRET_{OPERATEUR}.xlsx', engine='xlsxwriter') as writer:
        new_df.to_excel(writer, sheet_name='Lignes a ajouter aux flux', index=False)
       

def script_ch_benef(path_fact_enr, OPERATEURS, dim_projets, BASE_DIR):

    dim_projets_ = dim_projets.drop_duplicates(['projet_id'])
    for OPERATEUR in OPERATEURS:
        script_ch_benef_operateur(path_fact_enr, OPERATEUR, dim_projets_, BASE_DIR)

