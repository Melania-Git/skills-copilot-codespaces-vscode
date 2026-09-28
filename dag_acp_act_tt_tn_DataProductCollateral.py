# ============================================================================
# CONFIGURACIÓN DE DAGS
# ============================================================================
# NOTA: No es necesario definir global_config ni operator_config.
#       El factory usa configuración por defecto automáticamente.
#       Solo define configs personalizadas si necesitas overrides específicos.
#
#                              PROPOSITO
#   Orquestar el flujo para cargar datos a los data product de Collateral.
# -----------------------------------------------------------------------------
#                            MODIFICACIONES
# -----------------------------------------------------------------------------
#   FECHA           AUTOR          OBSERVACIÓN
#   18/05/2026      joharami       Desarrollo Inicial
#   28/09/2026      mzhinino       Inclusión DP Party Asset Direcotry
# ============================================================================
import sys
from airflow.models import Variable

# Configuración de rutas antes de los imports locales para evitar errores
path_utils = Variable.get("PATH_DAG_UTILS", default_var="./dags/dags_utils")
sys.path.append(f"{path_utils}")

import factory_zc_zp as factory

P_RUTA_PROYECTO = "/home/cda-bian-ope-custodycollateraldocs-datos/transformaciones"
ENVIO_CORREO = "0"

# Seleccionar ambiente desde Variable de Airflow (default: Produccion)
p_entorno = Variable.get("P_ENVIROMENT", default_var="Produccion")
p_ruta_primaria_ds = Variable.get("P_RUTA_PRIMARIA_DATASETS", default_var="/ruta/")
p_ruta_data_set = f"{p_ruta_primaria_ds}/acp/act/"

_BASE_FLOWS = [
    {
        "asset": "pl-transformacion-Acp-Zp-TNCollateralAllocationManagement",
        "ruta_asset": P_RUTA_PROYECTO,
        "talla": "M",
        "nombre_tabla": "collateral_allocation_management",
        "prioridad": "1",
        "dataset_salida": (
            f"{p_ruta_data_set}col_all_mng.collateral_allocation_management"
        ),
    },
    {
        "asset": "pl-transformacion-Acp-Zp-TNCollateralAllocationManagementDetail",
        "ruta_asset": P_RUTA_PROYECTO,
        "talla": "M",
        "nombre_tabla": "collateral_allocation_management_detail",
        "prioridad": "2",
        "dataset_salida": (
            f"{p_ruta_data_set}col_all_mng.collateral_allocation_management_detail"
        ),
    },
    {
        "asset": "pl-transformacion-Acp-Zp-DPPartyAssetDirectory",
        "ruta_asset": P_RUTA_PROYECTO,
        "talla": "M",
        "nombre_tabla": "party_asset_directory",
        "prioridad": "3",
        "dataset_salida": (
            f"{p_ruta_data_set}par_ass_dir.party_asset_directory"
        ),
    },
]

_RESOURCES_BY_ENV = {
    "Desarrollo": ["Transf_CN_Custody_Collateral"],
    "Test": ["Transf_CN_Custody_Collateral", "datos_test"],
    "Produccion": ["Transf_CN_Custody_Collateral", "datos_prod"],
}

flujos_config = factory.build_flujos_config(
    base_flows=_BASE_FLOWS,
    resources_by_env=_RESOURCES_BY_ENV,
    envio_correo_st=ENVIO_CORREO,
)

p_days_back_from = 1
p_days_back_to = 1

environment_flow_config = factory.EnvironmentFlowConfig(flows_config=flujos_config)

tags_productos = [
    "tribe: adm",
    "cell: datos",
    "domain: acp",
    "type: transformacion_datos",
]

dag = factory.create_dag_from_environment_flows(
    dag_id="dag_acp_act_dataproduct_collateral",
    codigo_malla="ACP_ACT_137",
    environment_flow_config=environment_flow_config,
    selected_environment=p_entorno,
    schedule=[
        f"{p_ruta_data_set}ZP_BP_Acp_Act_TN_GestionGarantias",
        f"{p_ruta_data_set}ZP_BP_Acp_Act_TN_DetalleGarantia",
    ],
    tags=tags_productos,
    description=(
        f"DAG de orquestación de los flujos de data product collateral - "
        f"Ambiente: {p_entorno}"
    ),
    owner="joharami",
    depends_on_past=False,
    email_on_failure=False,
    email_on_retry=False,
    retries=3,
    retry_delay_minutes=2,
    catchup=False,
    max_active_runs=8,
    dagrun_timeout_minutes=300,
    start_year=2026,
    start_month=1,
    start_day=6,
    days_back_from=p_days_back_from,
    days_back_to=p_days_back_to,
)

globals()["dag_acp_act_dataproduct_collateral"] = dag
