from app.database.db import session
from sqlalchemy import and_
from app.parametros.gerencia.model.datos_personales_model import UsuarioDatosPersonales
from app.parametros.gerencia.model.usuario_auth_model import UsuarioAuth
from sqlalchemy.orm import aliased

class GestorExcel:
    def __init__(self) -> None:
        pass
    
    
    def obtener_id_usuario(self,identificacion):
        usuario_auth_alias = aliased(UsuarioAuth)
        usuario_datos_personales_alias = aliased(UsuarioDatosPersonales)
        
        id_usuario = (
        session.query(usuario_auth_alias.id)
            .join(usuario_datos_personales_alias, usuario_datos_personales_alias.id_usuario == usuario_auth_alias.id)
            .filter(
                and_(
                    usuario_datos_personales_alias.identificacion == identificacion,
                    usuario_auth_alias.estado == 1
                )
            )
            .first()
        )
        
        resultado_id_usuario = {'id_usuario': id_usuario[0]} if id_usuario is not None else {'id_usuario': None}
            
        return resultado_id_usuario