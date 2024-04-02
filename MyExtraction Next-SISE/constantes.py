dtype = {'pia_id': str,
             'operateur_id': str,
             'action_id': str,
             'procedure_id': str,
             'projet_id': str,
             'beneficiaire_id': str,
             'state_id': str,
         'effect_id': str,
 'period_id': str,
 'montant': float,
 'nature_de_cofinanceur': str,
 'type_de_cofinancement': str,
 'nature_de_cout_de_gestion': str,
 'type_de_cout_de_gestion': str,
 'type_de_montant': str,
 'chef_fil_de_projet': str,
 'statut_abandon_rejet': str,
 'source': str,
 'snapshot_date': str,
 'valeur_decumulee': str,
 'valeur_cumulee': str}

OPERATEURS = ['ADEME', 'ANAH', 'ANDRA', 'ANR', 'ANRU', 'ASP', 'BPI', 'CDC', 'CEA', 'CNES', 'DGAC', 'FAM', 'ONERA']

engagement_effect_id = ['Engagement Avances remboursables',
                 'Engagement DNC',
                 'Engagement Fonds de Garantie',
                 'Engagement Intérêts DNC',
                 'Engagement Prêts',
                 'Engagement Prises de participation',
                 'Engagement Subventions',
                 'Engagement Dotations Décennales']

columns_order = ['pia_id', 'operateur_id', 'action_id', 'procedure_id', 'projet_id', 'projet_id_sise', 'projet_nom' ,'beneficiaire_id', 'effect_id', 'period_id', 'montant', 'projet_abandon', 'projet_statut_date_abandon_rejet', 'projet_statut_enr',
                 'nature_de_cofinanceur', 'type_de_cofinancement', 'nature_de_cout_de_gestion', 'type_de_cout_de_gestion', 'type_de_montant', 'chef_fil_de_projet', 'statut_abandon_rejet', 'source', 'snapshot_date', 'valeur_decumulee', 'valeur_cumulee']




