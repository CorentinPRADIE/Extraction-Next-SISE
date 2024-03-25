import streamlit as st
import pandas as pd
import dask.dataframe as dd
import os
from constantes import OPERATEURS, engagement_effect_id, dtype, columns_order
from datetime import datetime
import time


st.set_page_config(
    page_title="myFact_enr",
    layout='wide'
)

st.session_state['DIR_IN'] = r"C:\Users\ADSL\Desktop\Dev Python\99. MyFact_enr\DIR_IN"
st.session_state['DIR_OUT'] = r"C:\Users\ADSL\Desktop\Dev Python\99. MyFact_enr\DIR_OUT"
st.session_state['DIR_PARQUET'] = r"C:\Users\ADSL\Desktop\Dev Python\99. MyFact_enr\DIR_PARQUET"

def main():
    st.write("# Extraction Next-SISE")
    st.write('---')

    st.write('### 1. Configuration')
    
    st.write("##### Dossiers d'entrée et de sortie")
    st.session_state['DIR_IN'] = st.text_input('Dossier d\'entrée',  st.session_state['DIR_IN'])
    st.session_state['DIR_OUT'] = st.text_input('Dossier de sortie', st.session_state['DIR_OUT'])
    st.session_state['DIR_EXTRACT'] = os.path.join(st.session_state['DIR_OUT'], f'Fact_enr par actions {datetime.now().strftime('%d-%m-%Y')}')
    
    st.write(f"##### Télécharger et déposer les fichiers `fact_financiers_enr.csv` et `dim_projets.csv` dans le dossier : `{st.session_state['DIR_IN']}`") 
    
    st.session_state['FACT_ENR_FILE_NAME'] = st.selectbox('Nom du fichier `fact_financier_enr`:', sorted(list(os.listdir(st.session_state['DIR_IN'])), reverse=True))
    st.session_state['DIM_PROJETS_FILE_NAME'] = st.selectbox('Nom du fichier `dim_projets`:', os.listdir(st.session_state['DIR_IN']))
    
    
    
    st.write('---')
    
    st.write('### 2. Options')
    st.session_state['OPERATEURS'] = st.multiselect("Selections des opérateurs:", OPERATEURS, OPERATEURS)
    st.session_state['uniquement_fin_conso'] = st.checkbox("Uniquement le financier consommation", True)
    st.session_state['montants_non_nulles'] = st.checkbox("Uniquement les montants non nulles", True)
    st.session_state['uniquement_engagements'] = st.checkbox("Uniquement les montants d'engagements", False)
    
    st.write('')
    
    
    start = st.button("Start", type='primary', use_container_width=True)
    st.write('')
    st.write('')
    if start:
        path_fact_enr = os.path.join(st.session_state['DIR_IN'], st.session_state['FACT_ENR_FILE_NAME'])
        path_dim_projets = os.path.join(st.session_state['DIR_IN'], st.session_state['DIM_PROJETS_FILE_NAME'])
        with st.status("Chargement...", expanded=True) as status:
            debut = time.perf_counter()
            # st.write('Conversion de la fact_enr en fichier parquet...')
            # path_fact_enr_parquet = write_fact_enr_to_parquet(path_fact_enr)

            # st.write('Chargement de la dim_projets...')
            # dim_projets = read_dim_projets(path_dim_projets)

            # write_actions(path_fact_enr_parquet, dim_projets)

            # ----
            out_dir_name = f'{st.session_state['DIR_OUT']}/Fact_enr_par_actions {datetime.now().strftime('%d-%m-%Y %Hh%M')}'
            st.session_state['out_dir_name'] = f'{st.session_state['DIR_OUT']}/Fact_enr_par_actions {datetime.now().strftime('%d-%m-%Y %Hh%M')}'

            fact_enr = dd.read_csv(path_fact_enr, dtype=dtype, delimiter=';')
            dim_projets = read_dim_projets(path_dim_projets)
            st.write('Jointure des tables `fact_financiers_enr` et `dim_projets`')
            fact_enr = join_dim_projets(fact_enr, dim_projets)
            st.write('Fin')
            st.write('Filtre operateur')
            operateurs_list = st.session_state['OPERATEURS']
            print(operateurs_list)
            fact_enr = fact_enr [ fact_enr['operateur_id'].isin(operateurs_list) ]
            print(fact_enr['action_id'].unique().compute())
            print(fact_enr.head())
            st.write('Fin')
            fact_enr.groupby(['operateur_id', 'action_id']).apply(process_and_save).compute()
            

            
            
            # # Assumant que fact_enr est déjà votre DataFrame Dask chargé et joint correctement
            
            # bar_operateur = st.progress(0)
            
            # st.write('Groupby...')
            # # Groupby par 'operateur_id' et 'action_id'
            # grouped = fact_enr.groupby(['operateur_id', 'action_id'])
            
            # # Itérer sur chaque groupe
            # for (operateur, action_id), group in grouped:
            #     # Mise à jour de la barre de progression de l'opérateur
            #     num_ope = operateurs.index(operateur)  # Trouver l'index de l'opérateur dans la liste
            #     bar_operateur.progress(float(num_ope + 1) / len(operateurs))
                
            #     # Préparation du répertoire de sortie
            #     dir_path = f'{out_dir_name}/{operateur}'
            #     os.makedirs(dir_path, exist_ok=True)
                
            #     # Définition du chemin du fichier de sortie
            #     if pd.isna(action_id):
            #         file_path = f'{dir_path}/Action Nulle.xlsx'
            #     else:
            #         file_path = f'{dir_path}/{action_id}.xlsx'
                
            #     # Tri et sauvegarde du groupe (après conversion en DataFrame Pandas)
            #     group.compute().sort_values(['projet_id', 'period_id', 'beneficiaire_id', 'effect_id'], na_position='last') \
            #         .to_excel(file_path, index=False)
            
            # ----
            fin = time.perf_counter()
            temps_execution = fin - debut
            hours = int(temps_execution // 3600)  
            minutes = int((temps_execution % 3600) // 60)  
            secondes = int(temps_execution % 60)  
            
            status.update(label=f"Les fichiers sont prets dans le dossier: `{out_dir_name}`(en {hours}h {minutes}m {secondes}s)", state="complete", expanded=False)



def process_and_save(fact_enr):
    out_dir_name = st.session_state['out_dir_name']
    operateur = fact_enr.iloc[0]['operateur_id']
    
    print(operateur)
    action_id = fact_enr.iloc[0]['action_id']
    print(action_id)
    print(operateur, action_id)
    
    dir_path = f'{out_dir_name}/{operateur}'
    os.makedirs(dir_path, exist_ok=True)
    file_path = f'{dir_path}/{action_id}.xlsx'
    fact_enr.sort_values(['projet_id', 'period_id', 'beneficiaire_id', 'effect_id'], na_position='last').to_excel(file_path, index=False)


def write_fact_enr_to_parquet(path_fact_enr):
    # if st.session_state['test']:
    #     file_test = 'fact_enr_06-03-2024.parquet'
    #     return f'{st.session_state['DIR_PARQUET']}/{file_test}'
    path_fact_enr_parquet = f'{st.session_state['DIR_PARQUET']}/fact_enr_{datetime.now().strftime('%d-%m-%Y')}.parquet'
    if os.path.exists(path_fact_enr_parquet):
        st.write(f'le fichier `{path_fact_enr_parquet}` existe déjà, pas de conversion nécessaire...')
        return path_fact_enr_parquet    
    fact_enr = dd.read_csv(path_fact_enr, dtype=dtype, delimiter=';')
    fact_enr.to_parquet(path_fact_enr_parquet, partition_on='operateur_id')
    return path_fact_enr_parquet

def load_fact_enr_parquet(path_fact_enr_parquet, operateur, dim_projets):
    filters = [('operateur_id', '==', operateur)]
    if st.session_state['uniquement_fin_conso']:
        filters.append(('type_de_montant', '==', 'Financier Consommation'))
    if st.session_state['montants_non_nulles']:
        filters.append(('montant', '!=', 0))
    if st.session_state['uniquement_engagements']:
        filters.append(('effect_id', 'in', engagement_effect_id))
        
    fact_enr_parquet = None
    fact_enr_parquet = pd.read_parquet(path_fact_enr_parquet, filters=filters)

    fact_enr_parquet = join_dim_projets(fact_enr_parquet, dim_projets)
        
    return fact_enr_parquet

def load_fact_enr_parquet_per_action(path_fact_enr_parquet, dim_projets, action_id, operateur_id='BPI'):
    filters = [('operateur_id', '==', operateur_id), ('action_id', '==', action_id)]
    if st.session_state['uniquement_fin_conso']:
        filters.append(('type_de_montant', '==', 'Financier Consommation'))
    if st.session_state['montants_non_nulles']:
        filters.append(('montant', '!=', 0))
    if st.session_state['uniquement_engagements']:
        filters.append(('effect_id', 'in', engagement_effect_id))
        

    fact_enr_parquet = pd.read_parquet(path_fact_enr_parquet, filters=filters)

    fact_enr_parquet = join_dim_projets(fact_enr_parquet, dim_projets)
        
    return fact_enr_parquet

def get_bpi_actions(path_fact_enr_parquet):
    filters = [('operateur_id', '==', 'BPI'), ('montant', '!=', 0)]
    fact_enr = pd.read_parquet(path_fact_enr_parquet, filters=filters)
    bpi_actions = fact_enr['action_id'].dropna().unique().tolist()
    # bpi_actions = [item for item in my_list if item != '<NA>']
    return bpi_actions
# 1
def read_dim_projets(path_dim_projets):
    dim_projets = pd.read_csv(path_dim_projets, usecols=['projet_id', 'projet_id_sise', 'projet_nom', 'projet_abandon', 'projet_statut_date_abandon_rejet', 'projet_statut_enr'], delimiter=';')
    dim_projets = dim_projets.drop_duplicates(subset=['projet_id'])
    return dim_projets

# 2
def join_dim_projets(_fact_enr, _dim_projets):
    _fact_enr = dd.merge(_fact_enr, _dim_projets, on='projet_id', how='left')
    _fact_enr = _fact_enr[columns_order]
    return _fact_enr
    

def write_actions(path_fact_enr_parquet, dim_projets):
    str_fin_conso = 'Uniquement_financier_conso' if st.session_state['uniquement_fin_conso'] else 'Tous_types_de_montants'
    str_montants = 'Montants_non_nulles' if st.session_state['montants_non_nulles'] else 'Avec_montants_nulles'
    str_eng = 'Uniquement_les_engagements' if st.session_state['uniquement_engagements'] else 'Eng_Contrac_Dec'

    out_dir_name = f'{st.session_state['DIR_OUT']}/Fact_enr_par_actions {datetime.now().strftime('%d-%m-%Y %Hh%M')}'
    os.makedirs(out_dir_name, exist_ok=True)
    
    with open(f'{out_dir_name}/PARAMETRES_EXTRACTION.txt', 'w') as fichier:
        fichier.write(f"Uniquement le financier consommation : {st.session_state['uniquement_fin_conso']}\n")
        fichier.write(f"Uniquement les montants non nulles : {st.session_state['montants_non_nulles']}\n")
        fichier.write(f"Uniquement les montants d'engagements : {st.session_state['uniquement_engagements']}\n")


    OPERATEURS = st.session_state['OPERATEURS']
    bar_operateur = st.progress(0)
    for num_ope, operateur in enumerate(OPERATEURS):

        if operateur == 'BPI':
            bar_operateur.progress(float((num_ope)/len(OPERATEURS)), text=f'Ecriture de {operateur} ({num_ope+1}/{len(OPERATEURS)})')
            bpi_actions = get_bpi_actions(path_fact_enr_parquet)
            
            bar_action = st.progress(0)
            for num_act, action_id in enumerate(bpi_actions):
                bar_action.progress(float(num_act/len(bpi_actions)), text=f'Ecriture de {action_id} ({num_act+1}/{len(bpi_actions)})')
                fact_enr_parquet_action = load_fact_enr_parquet_per_action(path_fact_enr_parquet, dim_projets, action_id=action_id)
                
                dir_path = f'{out_dir_name}/{operateur}'
                os.makedirs(dir_path, exist_ok=True)
                if pd.isna(action_id) and not fact_enr_parquet_action.empty:
                    file_path = f'/{dir_path}/Action Nulle.xlsx'
                else:
                    file_path = f'{dir_path}/{action_id}.xlsx'
                fact_enr_parquet_action.sort_values(['projet_id', 'period_id', 'beneficiaire_id', 'effect_id'], na_position='last') \
                    .to_excel(file_path, index=False)
            bar_action.empty()
            
        else:
            bar_operateur.progress(float((num_ope)/len(OPERATEURS)), text=f'Ecriture de {operateur} ({num_ope+1}/{len(OPERATEURS)})')
        
            fact_enr_parquet = load_fact_enr_parquet(path_fact_enr_parquet, operateur, dim_projets)
            action_ids = fact_enr_parquet['action_id'].sort_values().unique().tolist()
            
            bar_action = st.progress(0)
            for num_act, action_id in enumerate(action_ids):
                bar_action.progress(float(num_act/len(action_ids)), text=f'Ecriture de {action_id} ({num_act+1}/{len(action_ids)})')
                fact_enr_parquet_action = fact_enr_parquet [ fact_enr_parquet['action_id'] == action_id ]
        
                dir_path = f'{out_dir_name}/{operateur}'
                os.makedirs(dir_path, exist_ok=True)
                if pd.isna(action_id) and not fact_enr_parquet_action.empty:
                    file_path = f'/{dir_path}/Action Nulle.xlsx'
                else:
                    file_path = f'{dir_path}/{action_id}.xlsx'
                fact_enr_parquet_action.sort_values(['projet_id', 'period_id', 'beneficiaire_id', 'effect_id'], na_position='last') \
                    .to_excel(file_path, index=False)
        
            bar_action.empty()

main()
    


