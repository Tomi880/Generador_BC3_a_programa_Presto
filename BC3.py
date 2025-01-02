# Primero, instalar esta libreria: pip install pyodbc
# Segundo, instalar el driver de SQL Server: ODBC Driver 17 for SQL Server o el driver que tengas instalado de base de datos

import pyodbc

# Funciones Auxiliares
def formato_numero ( numero, decimales ):
    return f"{numero:.{decimales}f}"

def formato_numero_entero ( numero, decimales ):
    if numero == int ( numero ):
        return str ( int ( numero ) )
    return f"{numero:.{decimales}f}"

def maximo_caracteres ( cadena, maximo ):
    if len ( cadena ) > maximo:
        return cadena [ :maximo ]
    return cadena

# Conectar a la Base de Datos
def conectar_base_datos ( servidor, base_datos ):
    try:
        conexion = pyodbc.connect ( 
            f"DRIVER={{SQL Server}};" # o el driver que tengas instalado de base de datos
            f"SERVER={servidor};"
            f"DATABASE={base_datos};"
            f"Trusted_Connection=yes;"
         )
        print ( "Conexión exitosa a SQL Server." )
        return conexion
    except Exception as e:
        raise Exception ( f"Error al conectar a SQL Server: {e}" )

# Ejecutar Consulta
def ejecutar_consulta ( cursor, consulta ):
    try:
        cursor.execute ( consulta )
        return cursor.fetchall ( )
    except Exception as e:
        raise Exception ( f"Error al ejecutar la consulta: {e}" )

#
#   funciones que almacenan los datos de la base de datos, se almacena en base a los indices que tenga la consulta en la base de datos
#

#
#   Almacenar cabecera
#
def almacenar_cabecera(fila, cabecera, codigo_cabecera):
    if codigo_cabecera not in cabecera:
        cabecera[codigo_cabecera] = {
            "nombre_cabecera"               : fila [ 1 ],
            "fecha_inicio_obra"             : fila [ 21 ],
            "fecha_termino_obra"            : fila [ 20 ],
            "total_presupuesto_obra"        : formato_numero( fila [ 23 ], 2 ),
            "total_precio_unitario_cabecera": formato_numero( fila [ 26 ], 2 ),
            "nombre_obra"                   : fila [ 19 ] ,
            "codigo_presupuesto"            : fila [ 22 ],
            "actividades_principales"       : [ ]
        }
    return ( cabecera )

def AgregaActividadesPrincipalesEnCabecera ( act_principal_codigo, codigo_cabecera, cabecera ):
    if act_principal_codigo not in cabecera [ codigo_cabecera ][ "actividades_principales" ]:
        cabecera [ codigo_cabecera ][ "actividades_principales" ].append ( act_principal_codigo )
    return ( cabecera )

#
#   Almacenar actividad principal
#
def almacenar_actividad_principal(fila, actividades_principales, act_principal_codigo, codigo_cabecera):
    if act_principal_codigo not in actividades_principales:
        actividades_principales[act_principal_codigo] = {
            "codigo": act_principal_codigo,
            "nombre_actividad_principal"               : fila [ 3 ],
            "total_precio_unitario_actividad_principal": formato_numero( fila [ 25 ], 2 ),
            "fecha_inicio_obra"                        : fila [ 21 ],
            "codigo_cabecera"                          : codigo_cabecera,
            "grupos_actividades"                       : [ ]
        }
    return ( actividades_principales )

def AgregaGruposActividadesEnActividadPrincipal ( grupoCodigo, act_principal_codigo, actividades_principales ):
    if grupoCodigo not in actividades_principales [ act_principal_codigo ][ "grupos_actividades" ]:
        actividades_principales [ act_principal_codigo ][ "grupos_actividades" ].append ( grupoCodigo )
    return ( actividades_principales )

#
#   Almacenar grupo
#
def AlmacenarGrupo ( fila, grupoCodigo, gruposActividades, act_principal_codigo ):
    if grupoCodigo not in gruposActividades:
        gruposActividades [ grupoCodigo ] = {
            "codigo"                    : grupoCodigo,
            "descripcion_grupo"         : fila [ 5 ],
            "factor"                    : formato_numero_entero ( fila [ 12 ], 1 ),
            "fecha_inicio_obra"         : fila [ 21 ],
            "codigo_actividad_principal": act_principal_codigo,
            "total_presupuesto_obra"    : formato_numero ( fila [ 23 ], 2 ),
            "Precio_grupo_actividad"    : formato_numero ( fila [ 24 ], 2 ),
            "actividades"               : [ ]
        }
    return ( gruposActividades )

def AgregaActividadesEnGrupo ( actividadCodigo, grupoCodigo, gruposActividades ):
    if actividadCodigo not in gruposActividades [ grupoCodigo ][ "actividades" ]:
        gruposActividades [ grupoCodigo ][ "actividades" ].append ( actividadCodigo )
    return ( gruposActividades )

#
#   Almacenar actividades
#
def AlmacenarActividades ( fila, actividadCodigo, codigoGrupo, actividades ):
    if actividadCodigo not in actividades:
        actividades [ actividadCodigo ] = {
            "descripcion_actividad"     : maximo_caracteres ( fila [ 7 ], 64) ,
            "total_recurso_actividad"   : formato_numero ( fila [ 17 ], 2 ),
            "fecha_termino"             : fila [ 16 ],
            "factor"                    : formato_numero_entero ( fila [ 12 ], 1 ),
            "grupo"                     : codigoGrupo,
            "codigo_unidad_actividad"   : fila [ 18 ],
            "recursos"                  : [ ]
    }
    return ( actividades )

def AgregaRecursosEnGrupoActividad ( actividadCodigo, grupoCodigo, grupoActividades ):
    if actividadCodigo not in grupoActividades [ grupoCodigo ][ "actividades" ]:
        grupoActividades [ grupoCodigo ][ "actividades" ].append ( actividadCodigo )
    return ( grupoActividades )

#
#   Almacenar recursos
#
def AlmacenarRecursos(fila, resourceCode, activityCode, resources):
    if resourceCode not in resources:
        resources[resourceCode] = {
            "descripcion_recurso": maximo_caracteres( fila [ 9 ], 64 ),
            "unidad"             : fila [ 10 ],
            "Codigo_clase"       : fila [ 28 ],
            "usos"               : {  }
        }
    resources[resourceCode]["usos"][activityCode] = {
        "cantidad"     : formato_numero( fila [ 11 ], 2 ),
        "factor"       : formato_numero_entero( fila [ 12 ], 1 ),
        "total_recurso": formato_numero( fila [ 13 ], 2 ),
        "fecha"        : fila [ 15 ]
    }

    return ( resources )

def AgregaRecursosEnActividad ( resourceCode, activityCode, activities ):
    if resourceCode not in activities [ activityCode ][ "recursos" ]:
        activities [ activityCode ][ "recursos" ].append ( resourceCode )
    return ( activities )

# Mapeo de Codigo de Clase a BC3
MAPEO_CRECCODIGO_A_BC3 = {
    '1': 1,   # Mano de obra -> Mano de obra
    '2': 3,   # Materiales -> Materiales
    '3': 2,   # Maquinaria y equipos -> Maquinaria y medios auxiliares
    '4': 0,   # Fletes -> Sin clasificar
    '5': 5,   # Herramientas y elem. seguridad -> Clasificación de residuo
    '6': 0,   # Derechos e instalaciones -> Sin clasificar
    '7': 0,   # MO personal directivo -> Sin clasificar
    '8': 0,   # Urbanización -> Sin clasificar
    '9': 0,   # Subcontratos -> Sin clasificar
    'S': 0,   # Subactividad -> Sin clasificar
    '.': 0    # Sin clasificar -> Sin clasificar
}

# Procesar Datos Obtenidos
def procesar_datos ( filas ):
    cabecera                = {}
    actividades_principales = {}
    grupos_actividades      = {}
    actividades             = {}
    recursos                = {}

    for fila in filas:
        codigo_cabecera      = fila [ 0 ]
        act_principal_codigo = fila [ 2 ]
        grupo_codigo         = fila [ 4 ]
        actividad_codigo     = fila [ 6 ]
        recurso_codigo       = fila [ 8 ]

        # Almacenar cabecera
        cabecera                = almacenar_cabecera ( fila, cabecera, codigo_cabecera )
        cabecera                = AgregaActividadesPrincipalesEnCabecera ( act_principal_codigo, codigo_cabecera, cabecera )
        

        # Almacenar actividad principal
        actividades_principales = almacenar_actividad_principal (fila, actividades_principales, act_principal_codigo, codigo_cabecera )
        actividades_principales = AgregaGruposActividadesEnActividadPrincipal (  grupo_codigo, act_principal_codigo, actividades_principales )

        # Almacenar grupo
        grupos_actividades      = AlmacenarGrupo ( fila, grupo_codigo, grupos_actividades, act_principal_codigo )
        grupos_actividades      = AgregaActividadesEnGrupo ( actividad_codigo, grupo_codigo, grupos_actividades )

        # Almacenar actividad
        actividades             = AlmacenarActividades ( fila, actividad_codigo, grupo_codigo, actividades )
        grupos_actividades      = AgregaRecursosEnGrupoActividad ( actividad_codigo, grupo_codigo, grupos_actividades )

        # Almacenar recurso
        recursos                = AlmacenarRecursos ( fila, recurso_codigo, actividad_codigo, recursos )
        actividades             = AgregaRecursosEnActividad ( recurso_codigo, actividad_codigo, actividades )

    return cabecera, actividades_principales, grupos_actividades, actividades, recursos

#
#   Cabecera del archivo BC3
#
def cabesera_bc3 ( archivo_bc3 ):
    archivo_bc3.write ( "~V|el nombre de tu empresa o proyecto|FIEBDC-3/2020\\10012025|Generador BC3||ANSI||2||||\n" )
    archivo_bc3.write ( "~L||ESP\\Especificación\\TEC\\Características técnicas\\PRE\\Condiciones previas\\EJE\\Ejecución\\MED\\Medición\\CON\\Control\\SEG\\Seguridad y salud\\GMA\\Gestión ambiental\\NOR\\Normas de aplicación\\MAN\\Mantenimiento\\VAR\\Varios\\TEX\\Texto sin faceta\\|\n" )
    archivo_bc3.write ( "~K|\\2\\2\\2\\3\\2\\2\\2\\2\\CLP|0\\0\\0\\0\\0|3\\2\\3\\3\\2\\2\\2\\2\\2\\2\\2\\2\\CLP\\|\n" )

    return ( archivo_bc3 )

#
#   Datos de la obra en el grupo C
#
def escribir_datos_obra_C ( archivo_bc3, cabecera ):
    cabecera_obra = next(iter(cabecera.values()))
    archivo_bc3.write(f"~C|{cabecera_obra['codigo_presupuesto']}##||{cabecera_obra['nombre_obra']}|{cabecera_obra['total_presupuesto_obra']}|{cabecera_obra['fecha_inicio_obra']}|0|\n")

    return ( archivo_bc3 )

#
#   Datos de la obra en el grupo D
#
def escribir_datos_obra_D ( archivo_bc3, cabecera ):
    cabecera_obra = next(iter(cabecera.values()))
    cabecera_codigo = [f"{codigo}\\1\\1" for codigo in cabecera.keys()]
    cabecera_line = '\\'.join(cabecera_codigo)
    archivo_bc3.write(f"~D|{cabecera_obra['codigo_presupuesto']}##|{cabecera_line}\\|\n")

    return ( archivo_bc3 )

#
#   Datos de las cabeceras en el C
#
def escribir_cabeceras_C(archivo_bc3, codigo_cabecera, cabecera_data):
    archivo_bc3.write(f"~C|{codigo_cabecera}#||{cabecera_data['nombre_cabecera']}|{cabecera_data['total_precio_unitario_cabecera']}|{cabecera_data['fecha_inicio_obra']}|0|\n")

    return ( archivo_bc3 )

#
#   Datos de las cabeceras en el D
#
def escribir_cabeceras_D(archivo_bc3, codigo_cabecera, cabecera_data):
    actividades_principales_joined = [f"{act_principal_codigo}\\1\\1" for act_principal_codigo in cabecera_data['actividades_principales']]
    actividades_principales_line = '\\'.join(actividades_principales_joined)
    archivo_bc3.write(f"~D|{codigo_cabecera}#|{actividades_principales_line}\\|\n")

    return ( archivo_bc3 )

#
#   Datos de las actividades principales en el C
#
def escribir_actividades_principales_C(archivo_bc3, act_principal):
    archivo_bc3.write(f"~C|{act_principal['codigo']}#||{act_principal['nombre_actividad_principal']}|{act_principal['total_precio_unitario_actividad_principal']}|{act_principal['fecha_inicio_obra']}|0|\n")

    return ( archivo_bc3 )

#
#   Datos de las actividades principales en el D
#
def escribir_actividades_principales_D(archivo_bc3, act_principal):
    grupos_actividades_joined = [f"{grupo_codigo}\\1\\1" for grupo_codigo in act_principal['grupos_actividades']]
    grupos_actividades_line = '\\'.join(grupos_actividades_joined)
    archivo_bc3.write(f"~D|{act_principal['codigo']}#|{grupos_actividades_line}\\|\n")

    return ( archivo_bc3 )

#
#   Datos de los grupos de actividades en el C
#
def grupos_actividades_datos_C ( archivo_bc3, grupo ):
    archivo_bc3.write(f"~C|{grupo['codigo']}#||{grupo['descripcion_grupo']}|{grupo['Precio_grupo_actividad']}|{grupo['fecha_inicio_obra']}|0|\n")

    return ( archivo_bc3 )

#
#   Datos de los grupos de actividades en el D
#
def grupos_actividades_datos_D ( archivo_bc3 , grupo ):
    actividades_joined = [f"{actividad_codigo}\\1\\1" for actividad_codigo in grupo["actividades"]]
    actividades_line = '\\'.join(actividades_joined)
    archivo_bc3.write(f"~D|{grupo['codigo']}#|{actividades_line}\\|\n")

    return ( archivo_bc3 )

#
#   Estructurar los datos de las actividades en el C
#
def actividades_datos_C ( archivo_bc3,  actividad_codigo, actividad):
    archivo_bc3.write ( f"~C|{actividad_codigo}|{actividad [ 'codigo_unidad_actividad' ]}|{actividad [ 'descripcion_actividad' ]}|{actividad [ 'total_recurso_actividad' ]}|{actividad [ 'fecha_termino' ]}|0|\n" )

    return ( archivo_bc3 )

#
#   Estructurar los datos de las actividades en el D
#
def actividades_datos_D(archivo_bc3, recursos, actividad, actividad_codigo):
    recursos_line = []
    for recurso_codigo in actividad["recursos"]:
        uso = recursos[recurso_codigo]["usos"].get(actividad_codigo)
        if uso:
            recursos_line.append(f"{recurso_codigo}\\{uso['factor']}\\{uso['cantidad']}")
    recursos_line_str = '\\'.join(recursos_line)
    archivo_bc3.write(f"~D|{actividad_codigo}|{recursos_line_str}\\|\n")

    return ( archivo_bc3 )

#
#   Estructurar los datos de los recursos en el C y D
#
def recursos_datos_C(archivo_bc3, recurso_codigo, recurso, actividad_codigo):
    uso = recurso["usos"].get(actividad_codigo)
    if uso:
        tipo_recurso = MAPEO_CRECCODIGO_A_BC3.get(str(recurso['Codigo_clase']), 0)
        archivo_bc3.write(
            f"~C|{recurso_codigo}|{recurso['unidad']}|{recurso['descripcion_recurso']}|"
            f"{uso['total_recurso']}|{uso['fecha']}|{tipo_recurso}|\n"
        )


    return ( archivo_bc3 )

#
#   Recorrer los datos y almacenarlos en las funciones
#
def recorrer_datos(archivo_bc3, cabecera, actividades_principales, grupos_actividades, actividades, recursos):
    for codigo_cabecera, cabecera_data in cabecera.items():
        escribir_cabeceras_C(archivo_bc3, codigo_cabecera, cabecera_data)
        escribir_cabeceras_D(archivo_bc3, codigo_cabecera, cabecera_data)
        for act_principal_codigo in cabecera_data['actividades_principales']:
            act_principal = actividades_principales[act_principal_codigo]
            escribir_actividades_principales_C(archivo_bc3, act_principal)
            escribir_actividades_principales_D(archivo_bc3, act_principal)
            for grupo_codigo in act_principal['grupos_actividades']:
                grupo = grupos_actividades[grupo_codigo]
                grupos_actividades_datos_C(archivo_bc3, grupo)
                grupos_actividades_datos_D(archivo_bc3, grupo)
                for actividad_codigo in grupo["actividades"]:
                    actividad = actividades[actividad_codigo]
                    actividades_datos_C(archivo_bc3, actividad_codigo, actividad)
                    actividades_datos_D(archivo_bc3, recursos, actividad, actividad_codigo)
                    for recurso_codigo in actividad["recursos"]:
                        recurso = recursos[recurso_codigo]
                        recursos_datos_C(archivo_bc3, recurso_codigo, recurso, actividad_codigo)

    return ( archivo_bc3 )

# Generar Archivo BC3
def generar_bc3 ( cabecera, grupos_actividades, actividades, recursos, actividades_principales, archivo_salida ):
    try:
        with open ( archivo_salida, 'w', encoding='iso-8859-1' ) as archivo_bc3:

            cabesera_bc3 ( archivo_bc3 )

            escribir_datos_obra_C ( archivo_bc3, cabecera )

            escribir_datos_obra_D ( archivo_bc3, cabecera )

            recorrer_datos ( archivo_bc3, cabecera, actividades_principales, grupos_actividades, actividades, recursos)

        print ( f"Archivo BC3 generado correctamente: {archivo_salida}" )
    except Exception as e:
        raise Exception ( f"Error al generar el archivo BC3: {e}" )

# Función Principal
def obtener_datos_bc3 ( servidor, base_datos ):
    conexion = conectar_base_datos ( servidor, base_datos )
    cursor   = conexion.cursor ( )

    consulta =  ( """
        Aqui tu consulta SQL
        """ )
    
    filas = ejecutar_consulta ( cursor, consulta )

    return filas

def crear_informe_bc3 ( servidor, base_datos, archivo_salida ):
    filas = obtener_datos_bc3 ( servidor, base_datos )
    cabecera, actividades_principales, grupos_actividades, actividades, recursos = procesar_datos ( filas )
    generar_bc3 ( cabecera, grupos_actividades, actividades, recursos, actividades_principales, archivo_salida )

servidor        = 'tu_servidor'
base_datos      = 'tu_base_de_datos'
archivo_salida  = 'el_nombre_del_archivo.bc3'
crear_informe_bc3 ( servidor, base_datos, archivo_salida )