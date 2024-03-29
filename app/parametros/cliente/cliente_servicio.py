from fastapi import  UploadFile
import pandas as pd
import math
from app.database.db import session
from app.funcionalidades_archivos.funciones_archivos_excel import GestorExcel
from app.parametros.cliente.model.cliente_model import ProyectoCliente
from typing import List
from app.parametros.mensajes_resultado.mensajes import GlobalMensaje
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert


class Cliente:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        # data para la comparacion de informacion sacandola de la base de datos
        self.__obtener_clientes_existente = self.obtener()
        self.__obtener_clientes_existente_estado = self.obtener_cliente_estado()
        # data que se usara para hacer la comparacion informacion que envia el usuario a traves del excel
        self.__data_usuario_cliente = self.validacion_informacion_cliente_nit()['cliente_filtro_excel']
        
        
    def validacion_existe_informacion(self,obtener_clientes_existentes)->bool:
        return len(self.__data_usuario_cliente) > 0 and len(obtener_clientes_existentes)>0

    def __proceso_de_informacion_estructuracion(self):
        
            df = pd.read_excel(self.__file.file)
            
            df = df.dropna()
            # Imprimir las columnas reales del DataFrame
            df.columns = df.columns.str.strip()
            
            if df.isna().any().any():
                return {'resultado': [], 'duplicados': [],'cantidad_duplicados':0,'estado':0}
            
            selected_columns = ["ID Cliente (ERP)", "Cliente", "NIT"]

            df_excel = df[selected_columns]
            
            # if df_excel.empty:
            #     return {'resultado': [], 'duplicados': [],'cantidad_duplicados':0,'estado':0}
            
            # Cambiar los nombres de las columnas
            df_excel = df_excel.rename(
                    columns={
                    "ID Cliente (ERP)": "cliente_id_erp", 
                        "Cliente": "razon_social", 
                        "NIT": "identificacion"
                    }
                )
            
            df_excel["cliente_id_erp"] = df_excel["cliente_id_erp"].astype(str).str.strip()
            df_excel["razon_social"] = df_excel["razon_social"].astype(str).str.strip()
                
            # df_filtered = df_excel.dropna()
                
            duplicados_unidad_erp = df_excel.duplicated(subset='cliente_id_erp', keep=False)
            duplicados_razon_social = df_excel.duplicated(subset='razon_social', keep=False)
                # Filtrar DataFrame original
            resultado = df_excel[~(duplicados_unidad_erp | duplicados_razon_social)].to_dict(orient='records')
            duplicados = df_excel[(duplicados_unidad_erp | duplicados_razon_social)].to_dict(orient='records')
                
            duplicated = pd.DataFrame(duplicados)
                
            if duplicated.isnull().any(axis=1).any():
                lista_clientes = []
            else:
                lista_clientes = [{**item, 'identificacion': int(item['identificacion'])} if isinstance(item.get('identificacion'), (int, float)) and not math.isnan(item.get('identificacion')) else item for item in duplicados]
            
            cantidad_duplicados = len(lista_clientes)
            return {
                        'resultado':resultado,
                        'duplicados':lista_clientes[0] if cantidad_duplicados > 0 else [],
                        'cantidad_duplicados':cantidad_duplicados,
                        'estado':3 if cantidad_duplicados > 0 else 0
                    }
          
    def proceso_sacar_estado(self):
            
            cliente_registro = self.filtro_clientes_nuevos()['estado']
            cliente_nit_invalido = self.validacion_informacion_cliente_nit()['estado']
            clientes_actualizacion = self.filtro_cliente_actualizar()['estado']
            clientes_sin_cambios = self.obtener_no_sufrieron_cambios()['estado']
            clientes_excepciones = self.obtener_excepciones_datos_unicos()['estado']
            clientes_duplicados = self.__proceso_de_informacion_estructuracion()['estado']
            # Crear un conjunto con todos los valores de estado
            estados = {cliente_registro,cliente_nit_invalido, clientes_actualizacion, clientes_sin_cambios, clientes_excepciones,clientes_duplicados}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados if len(estados_filtrados) > 0 else [0]   
    
    def transacciones(self):
        try:   
            # validacion = self.validacion_existe_informacion(self.__obtener_clientes_existente)
            data_excel_filtro = self.validacion_informacion_cliente_nit()
            
            estado_id = self.proceso_sacar_estado()
            obtener_duplicados = self.__proceso_de_informacion_estructuracion()
            
            
            if len(self.__data_usuario_cliente) > 0:
            
                registro_clientes = self.filtro_clientes_nuevos()['respuesta']
                actualizar_clientes = self.filtro_cliente_actualizar()['respuesta']
                obtener_excepciones = self.obtener_excepciones_datos_unicos()['respuesta']
                
                log_transaccion_registro_gerencia = {
                            "log_transaccion_excel": {
                                'Respuesta':[
                                    {
                                        'registro':self.insertar_informacion(registro_clientes),
                                        'actualizar':self.actualizar_informacion(actualizar_clientes),
                                        'sin_cambios':self.obtener_no_sufrieron_cambios()['respuesta'],
                                    }
                                ],
                                'errores':{
                                            'excepciones':{'datos':obtener_excepciones,'mensaje':GlobalMensaje.EXCEPCIONES_GENEREALES.value} if len(obtener_excepciones) > 0 else [],
                                            'duplicados':{'datos':obtener_duplicados['duplicados'] ,'mensaje':GlobalMensaje.mensaje(obtener_duplicados['cantidad_duplicados'])} if len(obtener_duplicados['duplicados']) else [],
                                            'nit_invalidos':{'datos':data_excel_filtro['log'],'mensaje':GlobalMensaje.NIT_INVALIDO.value} if len(data_excel_filtro['log']) else []
                                }
                    
                            },
                            'estado':estado_id
                            
                        }
                    
                return log_transaccion_registro_gerencia
            
            
            estado_id.insert(0,0)
            dato_estado = list(set(estado_id))
            return {
                        'mensaje':GlobalMensaje.NO_HAY_INFORMACION.value,
                        'nit_invalidos':{'datos':data_excel_filtro['log'],'mensaje':GlobalMensaje.NIT_INVALIDO.value} if len(data_excel_filtro['log']) else [],
                        'duplicados': {'datos':obtener_duplicados['duplicados'] ,'mensaje':GlobalMensaje.mensaje(obtener_duplicados['cantidad_duplicados'])} if len(obtener_duplicados['duplicados']) else [],
                        'estado':dato_estado
                }  
        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
    
    def obtener(self) -> List:
        informacion_cliente = session.query(ProyectoCliente).all()
        # Convertir lista de objetos a lista de diccionarios
        cliente = [cliente.to_dict() for cliente in informacion_cliente]
        return cliente
    
    def obtener_cliente_estado(self,estado = 1) -> List:
        informacion_cliente = session.query(ProyectoCliente).filter_by(estado=estado).all()
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
        
        if len(self.__obtener_clientes_existente) == 0:
            return {'respuesta':self.__data_usuario_cliente ,'estado':1} if len(self.__data_usuario_cliente) > 0 else {'respuesta':self.__data_usuario_cliente ,'estado':0}
        
        validacion = self.validacion_existe_informacion(self.__obtener_clientes_existente)
        
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
        
        validacion = self.validacion_existe_informacion(self.__obtener_clientes_existente_estado)
        
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
                            (x['razon_social'].lower()  in set(df_obtener_clientes_existentes['razon_social'].str.lower() )) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['razon_social'].str.lower() == x['razon_social'].lower(), 'cliente_id_erp'].values[0]) 
                            or
                            (x['identificacion'] in set(df_obtener_clientes_existentes['identificacion'])) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['identificacion'] == x['identificacion'], 'cliente_id_erp'].values[0])
                        ), axis=1)
                    ]
            
            if cliente_actualizar.empty:
                return {'respuesta':[],'estado':0}
            
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
        obetener_existentes = self.__obtener_clientes_existente
        validacion = self.validacion_existe_informacion(obetener_existentes)
        if validacion:
            
            df_clientes = pd.DataFrame(self.__data_usuario_cliente)
            df_obtener_clientes_existentes = pd.DataFrame(obetener_existentes)
            
            df_clientes["cliente_id_erp"] = df_clientes["cliente_id_erp"]
            df_obtener_clientes_existentes["cliente_id_erp"] = df_obtener_clientes_existentes["cliente_id_erp"]
            
            df_clientes["razon_social"] = df_clientes["razon_social"]
            df_obtener_clientes_existentes["razon_social"] = df_obtener_clientes_existentes["razon_social"]
            
            resultado = pd.merge(df_clientes, df_obtener_clientes_existentes, how='inner', on=['cliente_id_erp','razon_social', 'identificacion'])
            # df_filtrado = df_clientes[df_clientes.isin(df_obtener_clientes_existentes.to_dict('list')).all(axis=1)]
            clientes_sin_ningun_cambio = resultado.to_dict(orient='records')
        else:
            clientes_sin_ningun_cambio = []

        return {'respuesta':clientes_sin_ningun_cambio,'estado':3 if len(clientes_sin_ningun_cambio) > 0 else 0} 
    
    def obtener_excepciones_datos_unicos(self):
        validacion = self.validacion_existe_informacion(self.__obtener_clientes_existente)
        
        if validacion:
            
            df_clientes,df_obtener_clientes_existentes = self.obtener_data_serializada()
            
            excepciones = df_clientes[
            df_clientes.apply(
                lambda x: (
                    (
                        (x['razon_social'].lower() in df_obtener_clientes_existentes['razon_social'].str.lower().values) and
                        (x['cliente_id_erp'].lower() != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['razon_social'].str.lower() == x['razon_social'].lower(), 'cliente_id_erp'].str.lower().values[0])
                    ) or
                    (
                        (x['identificacion'] in df_obtener_clientes_existentes['identificacion']) and
                        (x['cliente_id_erp'].lower() != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['identificacion'] == x['identificacion'], 'cliente_id_erp'].str.lower().values[0])
                    )
                ) 
                , axis=1
            ) 
            | df_clientes['identificacion'].isin(df_obtener_clientes_existentes['identificacion'].values) 
            | df_clientes['cliente_id_erp'].str.lower().isin(df_obtener_clientes_existentes['cliente_id_erp'].str.lower().values)
        ]
            
            cliente_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            filtro_actualizacion = self.filtro_cliente_actualizar()['respuesta']
        
            
            filtrar_las_actualizaciones = self.filtro_de_excpeciones(cliente_filtro,filtro_actualizacion,excepciones)
            
            if len(filtrar_las_actualizaciones['respuesta']) > 0 or filtrar_las_actualizaciones['estado']:
                return {'respuesta': filtrar_las_actualizaciones['respuesta'], 'estado' : 3 if len(filtrar_las_actualizaciones['respuesta'])>0 else 0}

            resultado_actualizacion = excepciones.to_dict(orient='records')
        else:   
            resultado_actualizacion = []
        
        return  {'respuesta':resultado_actualizacion,'estado':3 if len(resultado_actualizacion) > 0 else 0} 
    
    def insertar_informacion(self, novedades_unidad_organizativa: List):
        cantidad_clientes_registrados = len(novedades_unidad_organizativa)
        if cantidad_clientes_registrados > 0:
            
            insertar_informacion = insert(ProyectoCliente,novedades_unidad_organizativa)
            session.execute(insertar_informacion)
            session.commit()
  
            return {'mensaje': f'Se han realizado {cantidad_clientes_registrados} registros exitosos.' if cantidad_clientes_registrados > 1 else  f'Se ha registrado un ({cantidad_clientes_registrados}) cliente exitosamente.'}
        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        cantidad_clientes_actualizados = len(actualizacion_gerencia_unidad_organizativa)
        if cantidad_clientes_actualizados > 0:
            session.bulk_update_mappings(ProyectoCliente, actualizacion_gerencia_unidad_organizativa)
            session.commit()
            return  {'mensaje': f'Se han realizado {cantidad_clientes_actualizados} actualizaciones exitosamente.' if cantidad_clientes_actualizados > 1 else  f'Se ha actualizado un ({cantidad_clientes_actualizados}) cliente exitosamente.'}
        return "No se han actualizado datos"


    def obtener_data_serializada(self):
        
            df_clientes = pd.DataFrame(self.__data_usuario_cliente)
            df_obtener_clientes_existentes = pd.DataFrame(self.__obtener_clientes_existente)
            
            df_clientes["cliente_id_erp"] = df_clientes["cliente_id_erp"]
            df_obtener_clientes_existentes["cliente_id_erp"] = df_obtener_clientes_existentes["cliente_id_erp"]
            
            df_clientes["razon_social"] = df_clientes["razon_social"]
            df_obtener_clientes_existentes["razon_social"] = df_obtener_clientes_existentes["razon_social"]
            
            return df_clientes,df_obtener_clientes_existentes


    def filtro_de_excpeciones(self,cliente_filtro,filtro_actualizacion,excepciones:pd.DataFrame):

        if len(filtro_actualizacion) > 0 and len(cliente_filtro) > 0:
            df_ceco_filtro = pd.DataFrame(filtro_actualizacion)
            df_cliente = pd.DataFrame(cliente_filtro)
            df_filtrado = excepciones[
                ~excepciones[['razon_social']].isin(df_ceco_filtro[['razon_social']].to_dict('list')).all(axis=1) &
                ~excepciones[['cliente_id_erp', 'razon_social', 'identificacion']].isin(df_cliente[['cliente_id_erp', 'razon_social', 'identificacion']].to_dict('list')).all(axis=1)
            ]
            
            filtro_combinado = df_filtrado.to_dict(orient='records')
            return {'respuesta': filtro_combinado, 'estado': 3} 
        elif len(filtro_actualizacion) > 0:
            df_ceco_filtro = pd.DataFrame(filtro_actualizacion)
            df_filtrado = excepciones[~excepciones[['razon_social']].isin(df_ceco_filtro[['razon_social']].to_dict('list')).all(axis=1)]
            filtro_ceco = df_filtrado.to_dict(orient='records')
            return {'respuesta': filtro_ceco, 'estado': 3} 
        elif len(cliente_filtro) > 0:
            
            df_cliente = pd.DataFrame(cliente_filtro)
            df_filtrado = excepciones[~excepciones[['cliente_id_erp', 'razon_social', 'identificacion']].isin(df_cliente[['cliente_id_erp', 'razon_social', 'identificacion']].to_dict('list')).all(axis=1)]
            
            filtro_cliente_actualizacion = df_filtrado.to_dict(orient='records')

            return {'respuesta': filtro_cliente_actualizacion, 'estado': 3} 
        else:
            return {'respuesta': [], 'estado': 0}