from enum import Enum


class MensajeAleratGerenica(Enum):
    GERENCIA_EXCEPCION = "El nombre de la Gerencia o la unidad gerencia ya se encuentran registrado. Por favor, verifique que no se encuentren registradas."
    NIT_INVALIDO = 'La información digitada en campo Gerente, debe contener solo números.Por favor, verifique que todos los valores sean numéricos'
    NIT_NO_ENCONTRADO = 'La cedula digitada no se encuentra registrada en el sistema, Por favor verifique que el Gerente exista o se encuentre activo.'

    def mensaje(mensaje):
        return  f'La información se encuentra duplicada {mensaje} veces en la columna. Por favor, verifique los datos registrados.'
    
    
class GlobalMensaje(Enum):
    NO_HAY_INFORMACION = 'No hay informacion para realizar el proceso, por favor verifique que se encuentren parametros registrados o no se enucentren inactivos.'
    NIT_INVALIDO = 'La información digitada en campo Gerente, debe contener solo números.Por favor, verifique que todos los valores sean numéricos'
    NIT_NO_ENCONTRADO = 'La cedula digitada no se encuentra registrada en el sistema, Por favor verifique que el Gerente exista o se encuentre activo.'
    EXCEPCIONES_GENEREALES = 'La informacion digitada ya se encuentra parametrizada. Por favor, verifique escoger una razon social o cedula que no se ecuentre registrada.'
    # CLIENTE_NO_EXISTEN = 'El campo ID Cliente no existe o no se encuentra activo.Por favor, verifique que el id cliente exista.'
    # ID_PROYECTO = 'El campo ID Proyecto no existe o no se encuentra activo.Por favor, verifique que el id proyecto exista.'
    
    def no_existen(campo)-> str:
        return f'El campo {campo} no se encuentra asociado o activo.Por favor, verifique que el {campo} exista dentro de la parametrizacion'
    
    def mensaje(mensaje)-> str:
        return  f'La información se encuentra duplicada {mensaje} veces en la columna. Por favor, verifique los datos registrados.'


class DireccionMensaje(Enum):
    EXCEPCION_DATOS_UNICOS = 'La informacion digitada ya se encuentra parametrizada. Por favor, verifique escoger una Direccion que no se ecuentre registrada.'
    EXCEPCION_NO_EXISTE = 'La informacion digitada no se encuentra parametrizada. Por favor, verifique escoger una Direccion se ecuentre registrada o activa.'

class ProyectosMensaje(Enum):
    GERENCIAS_NO_VINCULADAS = 'Por favor,revisar que la gerencia o direccion existan o se ecuentren activas y que la gerencia se ecuntre vinculada a una Direccion.'
    
    
    
class CecoMensaje(Enum):
    EXCEPCION_CECO_UNICO = 'La informacion digitada ya se encuentra parametrizada. Por favor, verifique escoger un nombre de proyecto que actualmente no se ecuentre registrado.'