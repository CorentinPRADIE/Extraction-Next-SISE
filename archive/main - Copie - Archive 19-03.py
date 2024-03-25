import streamlit as st
import pandas as pd
import dask.dataframe as dd
import os
from constantes import OPERATEURS, engagement_effect_id, dtype, columns_order
from script_ch_benef import script_ch_benef
from script_lignes_non_uniques import script_lignes_non_uniques
from datetime import datetime
import time



st.set_page_config(
    page_title="myFact_enr",
    layout='wide'
)



wd = os.getcwd()
st.session_state['DIR_IN'] = os.path.join(wd, "DIR_IN")
st.session_state['DIR_OUT'] = os.path.join(wd, "DIR_OUT")
st.session_state['DIR_PARQUET'] = os.path.join(wd, "DIR_PARQUET")

table_fichiers = pd.DataFrame({'Nom du fichier a telecharger sur MinIO' : ['dim_projets.csv', 'fact_financiers_enr.csv'], 'Dossier où déposer le fichier': [st.session_state['DIR_IN'], st.session_state['DIR_IN']]})

def menu():
    st.write("# Extraction Next-SISE")
    st.write("## par Corentin PRADIE")
    
    st.write('---')
    st.write(f"##### 1. Télécharger et déposer les fichiers `fact_financiers_enr.csv` et `dim_projets.csv` dans le dossier : `{st.session_state['DIR_IN']}`") 
    st.table(table_fichiers)
    st.write('## 1. Configuration')
    
    st.session_state['FACT_ENR_FILE_NAME'] = st.selectbox('Nom du fichier `fact_financier_enr`:', sorted(list(os.listdir(st.session_state['DIR_IN'])), reverse=True))
    st.session_state['DIM_PROJETS_FILE_NAME'] = st.selectbox('Nom du fichier `dim_projets`:', os.listdir(st.session_state['DIR_IN']))
    
    st.write('---')
    
    st.write('## 2. Options')
    st.write('#### Selections des opérateurs :')
    st.session_state['OPERATEURS'] = st.multiselect('', OPERATEURS, OPERATEURS, label_visibility='collapsed')
    with st.expander("Fichiers de sortie :"):
        st.write('#### Type de montants :')
        st.session_state['FIN_CONSO'] = st.checkbox("Une extraction du Financier Consommation", True)
        st.session_state['COFI'] = st.checkbox("Une extraction du Financier Cofinancement", False)
        st.session_state['BRUTE'] = st.checkbox("Une extraction brute (sans filtres)", False)
        st.write('#### Scripts :')
    
        st.session_state['script_ch_benef'] = st.checkbox("Script de dectection de changement de SIRET avec un SIREN différent", True)
        st.session_state['script_lignes_non_uniques'] = st.checkbox("Script de dectection de lignes non uniques sur le quadruplon ['projet_id', 'period_id', 'siren', 'effect_id']", True)
    
    st.write('')
    st.write('---')
    st.write("## 3. Lancer l'extraction")

def main():
    
    menu()
    
    start = st.button("Start", type='primary', use_container_width=True)
    st.write('')
    st.write('')
    if start:
        OPERATEURS_SELECTED =  st.session_state['OPERATEURS']
        if not OPERATEURS_SELECTED:
            st.write('Veuillez selectionner au moins un opérateur')
        else:
            path_fact_enr = os.path.join(st.session_state['DIR_IN'], st.session_state['FACT_ENR_FILE_NAME'])
            path_dim_projets = os.path.join(st.session_state['DIR_IN'], st.session_state['DIM_PROJETS_FILE_NAME'])
            with st.status("Chargement...", expanded=True) as status:
                debut = time.perf_counter()
                
                DIR_PARENT = f'{st.session_state['DIR_OUT']}/Fact_enr_par_actions {datetime.now().strftime('%d-%m-%Y %Hh%M')}'
                os.makedirs(DIR_PARENT, exist_ok=True)
                
                st.write('Conversion de la fact_enr en fichier parquet...')
                path_fact_enr_parquet = write_fact_enr_to_parquet(path_fact_enr, OPERATEURS_SELECTED)

                st.write('Chargement de la dim_projets...')
                dim_projets = read_dim_projets(path_dim_projets)

                if st.session_state['script_lignes_non_uniques']:
                    st.write("Script de dectection de lignes non uniques sur le quadruplon ['projet_id', 'period_id', 'siren', 'effect_id']...")
                    script_lignes_non_uniques(path_fact_enr_parquet, OPERATEURS_SELECTED, dim_projets, BASE_DIR=os.path.join(DIR_PARENT, 'Lignes non uniques sur [projet_id, period_id, siren, effect_id]'))
                
                write_actions(path_fact_enr_parquet, dim_projets, DIR_PARENT, OPERATEURS_SELECTED)
                
                hours, minutes, secondes = get_time(debut)
                st.write(f'Fact_enr par actions prets dans le dossier `{st.session_state['DIR_OUT']}` (en {hours}h {minutes}m {secondes}s)')

                
                if st.session_state['script_ch_benef']:
                    st.write('Script de dectection de changement de SIRET avec un SIREN différent...')
                    script_ch_benef(path_fact_enr_parquet,OPERATEURS_SELECTED, dim_projets, BASE_DIR=os.path.join(DIR_PARENT, 'Changements de SIRET'))

                
                hours, minutes, secondes = get_time(debut)
                status.update(label=f"Les fichiers sont prets dans le dossier: `{st.session_state['DIR_OUT']}`(en {hours}h {minutes}m {secondes}s)", state="complete", expanded=False)

               


def write_fact_enr_to_parquet(path_fact_enr, OPERATEURS_SELECTED):
    path_fact_enr_parquet = f'{st.session_state['DIR_PARQUET']}/fact_enr_{datetime.now().strftime('%d-%m-%Y %Hh%M')}.parquet' 
    fact_enr = dd.read_csv(path_fact_enr, dtype=dtype, delimiter=';')
    fact_enr = fact_enr[fact_enr['operateur_id'].isin(OPERATEURS_SELECTED)]
    fact_enr = fact_enr.dropna(subset=['action_id'])
    fact_enr['action_id'] = fact_enr['action_id'].map_partitions(lambda x: x.str.slice(0, 70).str.replace(' ', '_'))
    fact_enr.to_parquet(path_fact_enr_parquet, partition_on=['operateur_id', 'action_id'])
    return path_fact_enr_parquet



def load_fact_enr_parquet_per_action(path_fact_enr_parquet, dim_projets, operateur_id, action_id):
    filters = [('operateur_id', '==', operateur_id), ('action_id', '==', action_id)]
        
    fact_enr_parquet = pd.read_parquet(path_fact_enr_parquet, filters=filters)
    fact_enr_parquet = fact_enr_parquet [ fact_enr_parquet['montant'] != 0 ]
    fact_enr_parquet = join_dim_projets(fact_enr_parquet, dim_projets)
    
    return fact_enr_parquet

def get_action_ids(path_fact_enr_parquet, operateur_id):
    filters = [('operateur_id', '==', operateur_id)]
    fact_enr = pd.read_parquet(path_fact_enr_parquet, filters=filters, columns=['operateur_id', 'action_id'])
    fact_enr = fact_enr.dropna(subset=['action_id'])
    action_ids = fact_enr['action_id'].unique().tolist()
    return action_ids

def read_dim_projets(path_dim_projets):
    dim_projets = pd.read_csv(path_dim_projets, usecols=['projet_id', 'projet_id_sise', 'projet_nom', 'projet_abandon', 'projet_statut_date_abandon_rejet', 'projet_statut_enr'], delimiter=';')
    dim_projets = dim_projets.drop_duplicates(subset=['projet_id'])
    return dim_projets

def join_dim_projets(_fact_enr, _dim_projets):
    _fact_enr = pd.merge(_fact_enr, _dim_projets, on='projet_id', how='left')
    _fact_enr = _fact_enr[columns_order]
    return _fact_enr
    

def write_actions(path_fact_enr_parquet, dim_projets, DIR_PARENT, OPERATEURS_SELECTED):
    
    OPERATEURS = OPERATEURS_SELECTED
    FIN_CONSO = st.session_state['FIN_CONSO']
    COFI = st.session_state['COFI']
    BRUTE = st.session_state['BRUTE']
    
    bar_operateur = st.progress(0)
    for num_ope, operateur_id in enumerate(OPERATEURS):

        bar_operateur.progress(float((num_ope)/len(OPERATEURS)), text=f'Ecriture de {operateur_id} ({num_ope+1}/{len(OPERATEURS)})')
        action_ids = get_action_ids(path_fact_enr_parquet, operateur_id)

        if FIN_CONSO:
            FIN_CONSO_DIR = f'{DIR_PARENT}/Financier Consommation/{operateur_id}'
            os.makedirs(FIN_CONSO_DIR, exist_ok=True)
    
            ENG_DIR = f'{DIR_PARENT}/Financier Consommation - Engagement uniquement/{operateur_id}'
            os.makedirs(ENG_DIR, exist_ok=True)
        if COFI:
            COFI_DIR = f'{DIR_PARENT}/Financier Cofinancement/{operateur_id}'
            os.makedirs(COFI_DIR, exist_ok=True)
        if BRUTE:
            BRUTE_DIR = f'{DIR_PARENT}/BRUTE/{operateur_id}'
            os.makedirs(BRUTE_DIR, exist_ok=True)
        
        bar_action = st.progress(0)
        for num_act, action_id in enumerate(action_ids):
            bar_action.progress(float(num_act/len(action_ids)), text=f'Ecriture de {action_id} ({num_act+1}/{len(action_ids)})')
            
            fact_enr_parquet_action = load_fact_enr_parquet_per_action(path_fact_enr_parquet, dim_projets, operateur_id, action_id)
            file_name = f'/{action_id}.xlsx'
            
            if FIN_CONSO:
                df_fin_conso = fact_enr_parquet_action[fact_enr_parquet_action['type_de_montant'] == 'Financier Consommation'].sort_values(['projet_id', 'period_id', 'beneficiaire_id', 'effect_id'], ascending=False, na_position='last')
                df_fin_conso [ df_fin_conso['effect_id'].isin(engagement_effect_id) ].to_excel(ENG_DIR + file_name, sheet_name='ENGAGEMENTS', index=False)
                
                df_fin_conso.to_excel(FIN_CONSO_DIR + file_name, sheet_name='FINANCIER CONSOMMATION', index=False)
            if COFI:
                df_cofi = fact_enr_parquet_action[fact_enr_parquet_action['type_de_montant'] == 'Financier Cofinancement'].sort_values(['projet_id', 'period_id'], ascending=False, na_position='last')
                df_cofi.to_excel(COFI_DIR + file_name, sheet_name='FINANCIER COFINANCEMENT', index=False)
                
            if BRUTE:
                df_brute = fact_enr_parquet_action.sort_values(['projet_id', 'period_id'], ascending=False, na_position='last')
                df_brute.to_excel(BRUTE_DIR + file_name, sheet_name='BRUTE', index=False)

            bar_action.empty()

def get_time(debut):
    fin = time.perf_counter()
    temps_execution = fin - debut
    hours = int(temps_execution // 3600)  
    minutes = int((temps_execution % 3600) // 60)  
    secondes = int(temps_execution % 60)  
    return hours, minutes, secondes



main()
    


