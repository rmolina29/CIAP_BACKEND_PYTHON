from fastapi import  UploadFile
import pandas as pd
import math
from app.database.db import session
from app.parametros.cliente.model.cliente_model import ProyectoCliente
from typing import List


class Cliente:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        # data para la comparacion de informacion sacandola de la base de datos
        self.__obtener_clientes_existente = self.obtener()
        # data que se usara para hacer la comparacion informacion que envia el usuario a traves del excel
        self.__data_usuario_cliente = self.validacion_informacion_cliente_nit()['cliente_filtro_excel']
        
        
    def validacion_existe_informacion(self)->bool:
        return len(self.__data_usuario_cliente) > 0 and len(self.__obtener_clientes_existente)>0

    def __proceso_de_informacion_estructuracion(self):
            df = pd.read_excel(self.__file.file)
            # Imprimir las columnas reales del DataFrame
            df.columns = df.columns.str.strip()
            
            selected_columns = ["ID Cliente (ERP)", "Nombre", "NIT"]

            df_excel = df[selected_columns]
            
            if df_excel.empty:
                return {'resultado': [], 'duplicados': []}
            
            # Cambiar los nombres de las columnas
            df_excel = df_excel.rename(
                    columns={
                    "ID Cliente (ERP)": "cliente_id_erp", 
                        "Nombre": "razon_social", 
                        "NIT": "identificacion"
                    }
                )
            df_excel["cliente_id_erp"] = df_excel["cliente_id_erp"]
            df_excel["razon_social"] = df_excel["razon_social"]
                
            df_filtered = df_excel.dropna()
                
            duplicados_unidad_erp = df_filtered.duplicated(subset='cliente_id_erp', keep=False)
            duplicados_razon_social = df_filtered.duplicated(subset='razon_social', keep=False)
                # Filtrar DataFrame original
            resultado = df_filtered[~(duplicados_unidad_erp | duplicados_razon_social)].to_dict(orient='records')
            duplicados = df_filtered[(duplicados_unidad_erp | duplicados_razon_social)].to_dict(orient='records')
                
            duplicated = pd.DataFrame(duplicados)
                
            if duplicated.isnull().any(axis=1).any():
                lista_gerencias = []
            else:
                lista_gerencias = [{**item, 'identificacion': int(item['identificacion'])} if isinstance(item.get('identificacion'), (int, float)) and not math.isnan(item.get('identificacion')) else item for item in duplicados]
                
            return {'resultado':resultado,'duplicados':lista_gerencias}
          
    def proceso_sacar_estado(self):
            
            cliente_registro = self.filtro_clientes_nuevos()['estado']
            cliente_nit_invalido = self.validacion_informacion_cliente_nit()['estado']
            clientes_actualizacion = self.filtro_cliente_actualizar()['estado']
            clientes_sin_cambios = self.obtener_no_sufrieron_cambios()['estado']
            clientes_excepciones = self.obtener_excepciones_datos_unicos()['estado']

            # Crear un conjunto con todos los valores de estado
            estados = {cliente_registro,cliente_nit_invalido, clientes_actualizacion, clientes_sin_cambios, clientes_excepciones}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados            
    
    def transacciones(self):
             
        validacion = self.validacion_existe_informacion()
        
        if validacion:
        
            data_excel_filtro = self.validacion_informacion_cliente_nit()
            obtener_duplicados = self.__proceso_de_informacion_estructuracion()
            registro_clientes = self.filtro_clientes_nuevos()['respuesta']
            actualizar_clientes = self.filtro_cliente_actualizar()['respuesta']
                    
            estado_id = self.proceso_sacar_estado()
            
            log_transaccion_registro_gerencia = {
                        "log_transaccion_excel": {
                            'Respuesta':[
                                {
                                    'registo':self.insertar_informacion(registro_clientes),
                                     'actualizar':self.actualizar_informacion(actualizar_clientes),
                                     'sin_cambios':self.obtener_no_sufrieron_cambios()['respuesta'],
                                }
                            ],
                            'errores':{
                                        'excepciones':self.obtener_excepciones_datos_unicos()['respuesta'],
                                        'duplicados':obtener_duplicados['duplicados'],
                                        'nit_invalidos':data_excel_filtro['log']
                            }
                
                        },
                        'estado':{
                            'id':estado_id
                        }
                    }
                
            return log_transaccion_registro_gerencia
            
        return { 'errores':{
                    'mensaje':'No hay informacion para realizar el proceso',
                    'nit_invalidos':self.validacion_informacion_cliente_nit()['log'],
                    'duplicados':self.__proceso_de_informacion_estructuracion()['duplicados'],'estado':3
            }}     
    
    
    def obtener(self) -> List:
        informacion_cliente = session.query(ProyectoCliente).all()
        # Convertir lista de objetos a lista de diccionarios
        cliente = [cliente.to_dict() for cliente in informacion_cliente]
        return cliente
    
    def validacion_informacion_cliente_nit(self):
        try:
            resultado_estructuracion = self.__proceso_de_informacion_estructuracion()
            
            if len(resultado_estructuracion['resultado'])!=0:
                
                cliente_log, cliente_filtro_excel = [], []
                [(cliente_filtro_excel if isinstance(item.get('identificacion'), (int,float)) and 0 < item['identificacion'] < 10**11 else cliente_log).append(item) for item in resultado_estructuracion['resultado']]
                return {'log': cliente_log, 'cliente_filtro_excel': cliente_filtro_excel,'estado':3} if len(cliente_log) > 0 else {'log': cliente_log, 'cliente_filtro_excel': cliente_filtro_excel,'estado':0}
            
            return {'log': [], 'cliente_filtro_excel': [],'estado':0}
        
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}") from e
    
    def filtro_clientes_nuevos(self):
        
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            
            df_clientes = pd.DataFrame(self.__data_usuario_cliente)
            df_obtener_clientes_existentes = pd.DataFrame(self.__obtener_clientes_existente)
            
            resultado = df_clientes[
                    ~df_clientes.apply(lambda x: 
                        ((x['cliente_id_erp'].lower() in set(df_obtener_clientes_existentes['cliente_id_erp'].str.lower())) 
                         or 
                         (x['razon_social'].lower() in set(df_obtener_clientes_existentes['razon_social'].str.lower()))
                         or
                         (x['identificacion'] in set(df_obtener_clientes_existentes['identificacion']))
                         ), axis=1)
                ]
                                
            clientes_registro = resultado.to_dict(orient='records')
        else:
            clientes_registro = []
            
        return {'respuesta':clientes_registro,'estado':1} if len(clientes_registro) > 0 else {'respuesta':clientes_registro,'estado':0}
    
    
    def filtro_cliente_actualizar(self):
        
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            
            df_clientes,df_obtener_clientes_existentes = self.obtener_data_serializada()

            resultado = pd.merge(
                df_clientes[['cliente_id_erp','razon_social','identificacion']],
                df_obtener_clientes_existentes[['id','cliente_id_erp','razon_social','identificacion']],
                left_on=['cliente_id_erp'],
                right_on=['cliente_id_erp'],
                how='inner',
            )
            
            resultado_final = resultado[['id', 'cliente_id_erp', 'razon_social_x', 'identificacion_x']].rename(columns={'razon_social_x':'razon_social','identificacion_x':'identificacion'})
            
            
            cliente_actualizar = resultado_final[
                          ~resultado_final.apply(lambda x: (
                            (x['razon_social'] in set(df_obtener_clientes_existentes['razon_social'])) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['razon_social'] == x['razon_social'], 'cliente_id_erp'].values[0]) 
                            or
                            (x['identificacion'] in set(df_obtener_clientes_existentes['identificacion'])) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['identificacion'] == x['identificacion'], 'cliente_id_erp'].values[0])
                        ), axis=1)
                    ]
            
            clientes_ = cliente_actualizar[['id','razon_social','identificacion']]
            resultado_actualizacion = clientes_.to_dict(orient='records')
            cliente_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            
            if len(cliente_filtro) != 0:
                df_cliente = pd.DataFrame(cliente_filtro)
                df_filtrado = clientes_[~clientes_[['razon_social','identificacion']].isin(df_cliente[['razon_social','identificacion']].to_dict('list')).all(axis=1)]
                filtro_cliente_actualizacion =  df_filtrado.to_dict(orient='records')
                return {'respuesta':filtro_cliente_actualizacion,'estado':2} if len(filtro_cliente_actualizacion) > 0 else {'respuesta':filtro_cliente_actualizacion,'estado':0}
            
        else:
            resultado_actualizacion = []
            
        return {'respuesta':resultado_actualizacion,'estado':2} if len(resultado_actualizacion) > 0 else {'respuesta':resultado_actualizacion,'estado':0}
            
    def obtener_no_sufrieron_cambios(self):
        validacion = self.validacion_existe_informacion()
        if validacion:
            
            df_clientes = pd.DataFrame(self.__data_usuario_cliente)
            df_obtener_clientes_existentes = pd.DataFrame(self.__obtener_clientes_existente)
            
            df_clientes["cliente_id_erp"] = df_clientes["cliente_id_erp"]
            df_obtener_clientes_existentes["cliente_id_erp"] = df_obtener_clientes_existentes["cliente_id_erp"]
            
            df_clientes["razon_social"] = df_clientes["razon_social"]
            df_obtener_clientes_existentes["razon_social"] = df_obtener_clientes_existentes["razon_social"]
            
            # resultado = pd.merge(df_clientes, df_obtener_clientes_existentes, how='inner', on=['razon_social', 'identificacion'])
            
            df_filtrado = df_clientes[df_clientes.isin(df_obtener_clientes_existentes.to_dict('list')).all(axis=1)]
            
            
            clientes_sin_ningun_cambio = df_filtrado.to_dict(orient='records')
        else:
            clientes_sin_ningun_cambio = []

        return {'respuesta':clientes_sin_ningun_cambio,'estado':3} if len(clientes_sin_ningun_cambio) > 0 else {'respuesta':clientes_sin_ningun_cambio,'estado':0}
    
    def obtener_excepciones_datos_unicos(self):
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_clientes,df_obtener_clientes_existentes = self.obtener_data_serializada()
            
            print(df_clientes)
            print(df_obtener_clientes_existentes)
            


            excepciones = df_clientes[
                df_clientes.apply(
                    lambda x: (
                        (
                            (x['razon_social'].lower() in df_obtener_clientes_existentes['razon_social'].str.lower().values) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['razon_social'].str.lower() == x['razon_social'].lower(), 'cliente_id_erp'].values[0])
                        ) or
                        (
                            (x['identificacion'] in df_obtener_clientes_existentes['identificacion']) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['identificacion'] == x['identificacion'], 'cliente_id_erp'].values[0])
                        )
                    ),
                    axis=1
                )
            ]

            resultado_actualizacion = excepciones.to_dict(orient='records')
      
        else:
            resultado_actualizacion = []
            
        return  {'respuesta':resultado_actualizacion,'estado':3} if len(resultado_actualizacion) > 0 else {'respuesta':resultado_actualizacion,'estado':0}
    
    def insertar_informacion(self, novedades_unidad_organizativa: List):
        if len(novedades_unidad_organizativa) > 0:
            # session.bulk_insert_mappings(ProyectoCliente, novedades_unidad_organizativa)
            return novedades_unidad_organizativa
        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            # session.bulk_update_mappings(ProyectoCliente, actualizacion_gerencia_unidad_organizativa)
            return actualizacion_gerencia_unidad_organizativa
        return "No se han actualizado datos"
    

    

    def obtener_data_serializada(self):
        
            df_clientes = pd.DataFrame(self.__data_usuario_cliente)
            df_obtener_clientes_existentes = pd.DataFrame(self.__obtener_clientes_existente)
            
            df_clientes["cliente_id_erp"] = df_clientes["cliente_id_erp"]
            df_obtener_clientes_existentes["cliente_id_erp"] = df_obtener_clientes_existentes["cliente_id_erp"]
            
            df_clientes["razon_social"] = df_clientes["razon_social"]
            df_obtener_clientes_existentes["razon_social"] = df_obtener_clientes_existentes["razon_social"]
            
            return df_clientes,df_obtener_clientes_existentes


