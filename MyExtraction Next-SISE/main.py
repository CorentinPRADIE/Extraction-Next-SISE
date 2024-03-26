import streamlit as st
import pandas as pd
import dask.dataframe as dd
import os
from constantes import OPERATEURS, engagement_effect_id, dtype, columns_order
from script_ch_benef import script_ch_benef
from script_lignes_non_uniques import script_lignes_non_uniques
from datetime import datetime
import time
import shutil


st.set_page_config(
    page_title="myFact_enr",
    layout='wide'
)


wd = os.getcwd()
st.session_state['DIR_IN'] = os.path.join(wd, "DIR_IN")
st.session_state['DIR_OUT'] = os.path.join(wd, "DIR_OUT")
st.session_state['DIR_PARQUET'] = os.path.join(wd, "DIR_PARQUET")

DIR_DIM_PROJETS = 'Déposez la dim_projets ici'
DIR_FACT_ENR = 'Déposez la fact_financier_enr ici'

PATH_DIR_DIM_PROJETS = os.path.join(st.session_state['DIR_IN'], DIR_DIM_PROJETS)
PATH_DIR_FACT_ENR = os.path.join(st.session_state['DIR_IN'],  DIR_FACT_ENR)

os.makedirs(st.session_state['DIR_PARQUET'], exist_ok=True)
os.makedirs(st.session_state['DIR_OUT'], exist_ok=True)
os.makedirs(PATH_DIR_DIM_PROJETS, exist_ok=True)
os.makedirs(PATH_DIR_FACT_ENR, exist_ok=True)



table_fichiers = pd.DataFrame({'Nom du fichier a telecharger sur MinIO' : ['dim_projets.csv', 'fact_financiers_enr.csv'], 'Dossier où déposer le fichier': [PATH_DIR_DIM_PROJETS, PATH_DIR_FACT_ENR]})

def menu():
    st.write("# Extraction Next-SISE")
    st.write("## par Corentin PRADIE")
    
    st.write('---')
    
    st.write(f"#### 1. Télécharger les fichiers `fact_financiers_enr.csv` et `dim_projets.csv` depuis MinIO :")
    st.write('Lien vers MinIO : \n - [MinIO prod](https://console-datafactory.fichiers-prod.sgpi.dfakto.com/login),\n- [MinIO pre-prod](https://console-datafactory.fichiers-preprod.sgpi.dfakto.com/login)') 

    st.write(f"#### 2. Déposez les tables `fact_financiers_enr.csv` et `dim_projets.csv` dans leurs dossiers respectifs :\n Veuillez à ce qu'il n'y ait qu'un fichier par dossier.")
    st.dataframe(table_fichiers, hide_index=True, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        dim_projets_button = st.button("Ouvrir le dossier où déposer la table `la dim_projets.csv`", on_click=lambda : os.system(f'explorer "{PATH_DIR_DIM_PROJETS}"'))
    with col2:
        fact_enr_button = st.button("Ouvrir le dossier où déposer la table `fact_financiers_enr.csv`", on_click=lambda : os.system(f'explorer "{PATH_DIR_FACT_ENR}"'))


    
    st.write('#### 3. Verification')

    st.write(f"dim_projets         : {' :white_check_mark: le fichier est bien déposé' if sum(1 for fichier in os.listdir(PATH_DIR_DIM_PROJETS) if fichier.endswith('.csv')) == 1 else ' :x: Verifier que le fichier est bien déposé et qu il n y ait qu un seul fichier'}")
    st.write(f"fact_financiers_enr : {' :white_check_mark: le fichier est bien déposé' if sum(1 for fichier in os.listdir(PATH_DIR_FACT_ENR) if fichier.endswith('.csv')) == 1 else ':x: Verifier que le fichier est bien déposé et qu il n y ait qu un seul fichier'}")
    
    st.write('---')
    
    st.write('## 2. Options')
    st.write('#### Selections des opérateurs :')
    st.session_state['OPERATEURS'] = st.multiselect('selection', OPERATEURS, OPERATEURS, label_visibility='collapsed')
    with st.expander("Fichiers de sortie :"):
        st.write('#### Type de montants :')
        st.session_state['FIN_CONSO'] = st.checkbox("Une extraction du Financier Consommation", True)
        st.session_state['COFI'] = st.checkbox("Une extraction du Financier Cofinancement", True)
        st.session_state['BRUTE'] = st.checkbox("Une extraction brute (sans filtres)", False)
        st.write('#### Scripts :')
    
        st.session_state['script_ch_benef'] = st.checkbox("Script de dectection de changement de SIRET avec un SIREN différent", True)
        st.session_state['script_lignes_non_uniques'] = st.checkbox("Script de dectection de lignes non uniques sur le quadruplon ['projet_id', 'period_id', 'siren', 'effect_id']", True)
    
    st.write('')
    st.write('---')
    st.write("## 3. Lancer l'extraction")

def main():
    
    menu()
    # Crée un bouton "Start" qui, une fois cliqué, déclenche le reste du processus.
    start = st.button("Start", type='primary', use_container_width=True)
    
    
    if start:
        # Récupère la liste des opérateurs sélectionnés stockés dans l'état de la session Streamlit.
        OPERATEURS_SELECTED =  st.session_state['OPERATEURS']
        if not OPERATEURS_SELECTED:
            st.write('Veuillez selectionner au moins un opérateur')
        else:
    
            PATH_FACT_ENR = os.path.join(PATH_DIR_FACT_ENR,  os.listdir(PATH_DIR_FACT_ENR)[0])
            PATH_DIM_PROJETS = os.path.join(PATH_DIR_DIM_PROJETS,  os.listdir(PATH_DIR_DIM_PROJETS)[0])
    
            with st.status("Chargement...", expanded=True) as status:
                debut = time.perf_counter()
                # Crée un dossier pour stocker les résultats, avec un nom basé sur la date et l'heure actuelles.
                DIR_PARENT = f'{st.session_state['DIR_OUT']}/Fact_enr_par_actions {datetime.now().strftime('%d-%m-%Y %Hh%M')}'
                os.makedirs(DIR_PARENT, exist_ok=True)
               
                # Convertit les données spécifiées en format Parquet et retourne le chemin du fichier résultant.
                PATH_FACT_ENR_parquet = write_fact_enr_to_parquet(PATH_FACT_ENR, OPERATEURS_SELECTED)
                
                # Charge le réferentiel projet
                dim_projets = read_dim_projets(PATH_DIM_PROJETS)

                # Ecris un fichier excel pour chaque action pour pouvoir "lire" la base de données avec Excel.
                write_actions(PATH_FACT_ENR_parquet, dim_projets, DIR_PARENT, OPERATEURS_SELECTED)
                
                # Exécute le script de détection de changement de SIRET si l'option est activée.
                if st.session_state['script_ch_benef']:
                    script_ch_benef(PATH_FACT_ENR_parquet,OPERATEURS_SELECTED, dim_projets, BASE_DIR=os.path.join(DIR_PARENT, 'Changements de SIRET'))
                    
                # Exécute le script de détection de lignes non uniques si l'option est activée.
                if st.session_state['script_lignes_non_uniques']:
                    script_lignes_non_uniques(PATH_FACT_ENR_parquet, OPERATEURS_SELECTED, dim_projets, BASE_DIR=os.path.join(DIR_PARENT, 'Lignes non uniques sur [projet_id, period_id, siren, effect_id]'))
                    
                
                hours, minutes, secondes = get_time(debut) 
		# Met à jour le message de statut pour indiquer que le processus est terminé.
                status.update(label=f"Les fichiers sont prets dans le dossier: `{st.session_state['DIR_OUT']}`(en {hours}h {minutes}m {secondes}s)", state="complete", expanded=False)
                shutil.rmtree(PATH_FACT_ENR_parquet)
               

# Crée le chemin du fichier Parquet de sortie avec un timestamp.
def write_fact_enr_to_parquet(PATH_FACT_ENR, OPERATEURS_SELECTED):
    
    # Lit le fichier CSV source, filtre les lignes par operateur_id sélectionné.
    PATH_FACT_ENR_parquet = f'{st.session_state['DIR_PARQUET']}/fact_enr_{datetime.now().strftime('%d-%m-%Y %Hh%M')}.parquet' 
    fact_enr = dd.read_csv(PATH_FACT_ENR, dtype=dtype, delimiter=';')
    fact_enr = fact_enr[fact_enr['operateur_id'].isin(OPERATEURS_SELECTED)]
    
    # Supprime les lignes où 'action_id' est manquant.
    fact_enr = fact_enr.dropna(subset=['action_id'])
    
    # Normalise 'action_id' en limitant sa longueur à 70 caractères et en remplaçant les espaces par des underscores.
    fact_enr['action_id'] = fact_enr['action_id'].map_partitions(lambda x: x.str.slice(0, 70).str.replace(' ', '_'))
    
    # Écrit le DataFrame filtré en fichier Parquet, partitionné par 'operateur_id' et 'action_id'.
    fact_enr.to_parquet(PATH_FACT_ENR_parquet, partition_on=['operateur_id', 'action_id'])
    
    return PATH_FACT_ENR_parquet



def load_fact_enr_parquet_per_action(PATH_FACT_ENR_parquet, dim_projets, operateur_id, action_id):
    # Applique des filtres pour charger uniquement les données correspondantes à un operateur_id et un action_id spécifiques.
    filters = [('operateur_id', '==', operateur_id), ('action_id', '==', action_id)]
    fact_enr_parquet = pd.read_parquet(PATH_FACT_ENR_parquet, filters=filters)

    # Exclut les lignes avec un montant égal à 0.
    fact_enr_parquet = fact_enr_parquet [ fact_enr_parquet['montant'] != 0 ]

    # Joint les données du referentiel projet.
    fact_enr_parquet = join_dim_projets(fact_enr_parquet, dim_projets)
    
    return fact_enr_parquet

def get_action_ids(PATH_FACT_ENR_parquet, operateur_id):
    
    # Joint les données chargées avec les informations des projets via la fonction join_dim_projets.
    filters = [('operateur_id', '==', operateur_id)]
    fact_enr = pd.read_parquet(PATH_FACT_ENR_parquet, filters=filters, columns=['operateur_id', 'action_id'])

    # Supprime les lignes où 'action_id' est manquant.
    fact_enr = fact_enr.dropna(subset=['action_id'])
    
    # Extrait et retourne la liste des identifiants uniques d'action.
    action_ids = fact_enr['action_id'].unique().tolist()
    
    return action_ids

def read_dim_projets(PATH_DIM_PROJETS):
    
    # Lit le fichier CSV des projets, en sélectionnant uniquement certaines colonnes.
    dim_projets = pd.read_csv(PATH_DIM_PROJETS, usecols=['projet_id', 'projet_id_sise', 'projet_nom', 'projet_abandon', 'projet_statut_date_abandon_rejet', 'projet_statut_enr'], delimiter=';')
    
    # Supprime les doublons basés sur 'projet_id' pour ne garder qu'une ligne par projet.
    dim_projets = dim_projets.drop_duplicates(subset=['projet_id'])
    
    return dim_projets

def join_dim_projets(_fact_enr, _dim_projets):
    # Jointure de la fact_financiers_enr et la dim_projets.
    _fact_enr = pd.merge(_fact_enr, _dim_projets, on='projet_id', how='left')
    # Réarrange les colonnes.
    _fact_enr = _fact_enr[columns_order]
    
    return _fact_enr
    

def write_actions(PATH_FACT_ENR_parquet, dim_projets, DIR_PARENT, OPERATEURS_SELECTED):
    
    # Initialise les variables et répertoires de sortie en fonction des sélections de l'utilisateur et de l'état de la session Streamlit.
    OPERATEURS = OPERATEURS_SELECTED
    FIN_CONSO = st.session_state['FIN_CONSO']
    COFI = st.session_state['COFI']
    BRUTE = st.session_state['BRUTE']

    # Itère sur chaque opérateur sélectionné, affichant la progression dans la barre de progression des opérateurs.
    bar_operateur = st.progress(0)
    for num_ope, operateur_id in enumerate(OPERATEURS):

        bar_operateur.progress(float((num_ope)/len(OPERATEURS)), text=f'Ecriture de {operateur_id} ({num_ope+1}/{len(OPERATEURS)})')
        # Recupère toutes les actions de l'operateur.
        action_ids = get_action_ids(PATH_FACT_ENR_parquet, operateur_id)

        # Pour chaque opérateur, crée les répertoires nécessaires pour stocker les fichiers générés, en fonction des options sélectionnées (FIN_CONSO, COFI, BRUTE).
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

        # Itère sur chaque identifiant d'action pour l'opérateur en cours, affichant la progression dans la barre de progression des actions.
        bar_action = st.progress(0)
        for num_act, action_id in enumerate(action_ids):
            bar_action.progress(float(num_act/len(action_ids)), text=f'Ecriture de {action_id} ({num_act+1}/{len(action_ids)})')

            
            fact_enr_parquet_action = load_fact_enr_parquet_per_action(PATH_FACT_ENR_parquet, dim_projets, operateur_id, action_id)
            file_name = f'/{action_id}.xlsx'

            # Pour chaque catégorie de données sélectionnée (Financier Consommation, Financier Cofinancement, BRUTE), effectue les opérations suivantes :
                # Filtre les données en fonction du type de montant.
                # Trie les données selon les critères spécifiés (projet_id, period_id, etc.).
                # Génère un fichier Excel dans le répertoire approprié avec les données triées et filtrées.
            
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

            # Vide la barre de progression des actions après le traitement de chaque action.
            bar_action.empty()

def get_time(debut):
    fin = time.perf_counter()
    temps_execution = fin - debut
    hours = int(temps_execution // 3600)  
    minutes = int((temps_execution % 3600) // 60)  
    secondes = int(temps_execution % 60)  
    return hours, minutes, secondes



main()
    


