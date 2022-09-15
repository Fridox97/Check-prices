import pandas as pd
from SSH import get_products
from SSH import update
from updateProducts import activateProducts, desactivateProducts
from DBclass import DbInfo
import datetime
import math

medianas = []
preview_data = pd.DataFrame
precio_productos_super = []
productos_sospechosos_por_super_mercado = []
super_market = {
    '3': 'Nature Fresh',
    '8': 'Supermercados Trebol',
    '37': 'Plaza Lama',
    '168': 'Supermercado Xtra',
    '169': 'Super Valerio',
    '178': 'Supermercado La Fuente Fun',
    '179': 'Supermercado Trebol',
    '427': 'Bohio',
    '428': 'El cafe de la abuela',
    '475': 'Recovery Wines and Beer',
    '476': 'BBQ Congelados Los Jardines',
    '551': 'BBQ congelados Pontezuela',
    '554': 'Carniceria El Dorado',
    '557': 'Carniceria Depot',
    '593': 'Sante Santiago',
    '645': 'Pet Hospital',
    '653': 'Beauty Supply',
    '657': 'Vocatus27',
    '660': 'vocatus colinas',
    '661': 'Perra Blanca Grill'
}

jeisonDB = DbInfo('localhost', 'jeison', 'jdiazsuperkiwi21j', 'suspiciusPrices', 3306, 'ec2-18-206-137-112.compute-1.amazonaws.com', 'ubuntu', 22,
                  './JeisonDiaz.pem')
kiwiDB = DbInfo('localhost', 'noelUsr', 'TG#2a12$', 'db_noelgrcr', 3306, 'ec2-18-225-14-113.us-east-2.compute.amazonaws.com', 'ubuntu', 22,
                './noelgrowcer.pem')


def diff_value():
    global medianas, precio_productos_super
    query = 'select * from tbl_products'
    products = get_products(kiwiDB, query, 'product_upc')

    query = 'select selprod_user_id, selprod_price, selprod_product_id from tbl_seller_products inner join tbl_shops on shop_user_id = ' \
            'selprod_user_id where ' \
            'selprod_product_id > 130 and selprod_user_id ' \
            '!= 46 and selprod_user_id != 180 and selprod_deleted = 0 and shop_active = 1 and shop_live = 1'

    seller_prod = get_products(kiwiDB, query, 'none')

    precio_productos_super = lookup_products_shop(products, seller_prod)

    precio_productos_super = remove_empty(precio_productos_super)

    lookup_mediana(precio_productos_super)

    remove_nan(medianas, precio_productos_super)

    add_percentage(precio_productos_super, medianas)

    # Activar la funcion de abajo si se quiere saber cuantos productos por supermercado y cuales productos estan malos. En la misma funcion pueden
    # cambiar el porciento del criterio.
    # suspicius_prices(precio_productos_super, medianas, products)

    precio_productos_super = pd.concat(precio_productos_super)

    load_data()

    compare_previews_data()

    activateProducts()
    desactivateProducts()


# Buscamos todas las tiendas que tengan los productos asociados
def lookup_products_shop(products, seller_prod):
    prod = []
    for i in products.index:
        aux = seller_prod[seller_prod['selprod_product_id'] == products['product_id'][i]]
        aux['selprod_price'] = aux['selprod_price'].astype(float)
        prod.append(remove_percentage_trebol(aux))

    return prod


# removemos el porciento extra del supermercado trebol
def remove_percentage_trebol(products):
    for i in products.index:
        if products.at[i, 'selprod_user_id'] == '179':
            products.at[i, 'selprod_price'] = products.at[i, 'selprod_price'] / 1.1

    return products


# removemos cualquier dataframe que no contenga informacion
def remove_empty(products):
    for i in range(len(products) - 1, -1, -1):
        if products[i].empty:
            del products[i]

    return products


# Removemos desviaciones estandar que sean NaN
def remove_nan(desviacion, producto):
    for i in range(len(desviacion) - 1, -1, -1):
        if math.isnan(desviacion[i]):
            del desviacion[i]
            del producto[i]


# Buscar promedio de precio de cada producto
def lookup_mediana(products):
    for i in range(len(products)):
        prod = products[i].sort_values(by=['selprod_price'])
        indixes = prod.index
        if len(prod.index) % 2:
            medio = math.floor(len(prod.index) / 2)
            medianas.append(prod.at[indixes[medio], 'selprod_price'])
        else:
            medio1 = len(prod.index) / 2
            medio2 = medio1 - 1

            mediana = (prod.at[indixes[medio1], 'selprod_price'] + prod.at[indixes[medio2], 'selprod_price']) / 2
            medianas.append(mediana)


# Comparar los promedios con sus valores en cada tienda en busca de diferencias desproporcionales.
def suspicius_prices(prices, mediana, products):
    cont = 0
    for x in range(len(mediana)):
        for y in prices[x].index:
            if prices[x].at[y, 'Porciento'] < 70:
                product_name = products.loc[products['product_id'] == prices[x].at[y, "selprod_product_id"], ['product_identifier']]
                super_market_id = prices[x].at[y, 'selprod_user_id']
                contar(super_market[super_market_id])
                cont += 1
                print(F'Negocio: {super_market[super_market_id]}\n'
                      F'Producto: {product_name.values[0][0]},\n'
                      F'precio:{prices[x].at[y, "selprod_price"]},\n'
                      F'Precio esperado: {mediana[x]}\n'
                      F'Porciento: {round(prices[x].at[y, "Porciento"], 2)}%\n')

    print(F'Hay un total de {cont} precios sospechosos\n'
          F'Por super mercado:\n'
          F'Bohio: {productos_sospechosos_por_super_mercado.count("Bohio")}\n'
          F'Plaza Lama: {productos_sospechosos_por_super_mercado.count("Plaza Lama")}\n'
          F'Supermercado Xtra: {productos_sospechosos_por_super_mercado.count("Supermercado Xtra")}\n'
          F'Supermercado La Fuente Fun: {productos_sospechosos_por_super_mercado.count("Supermercado La Fuente Fun")}\n'
          F'Supermercado Trebol: {productos_sospechosos_por_super_mercado.count("Supermercado Trebol")}\n'
          F'Recovery Wines and Beer: {productos_sospechosos_por_super_mercado.count("Recovery Wines and Beer")}\n'
          F'BBQ Congelados Los Jardines: {productos_sospechosos_por_super_mercado.count("BBQ Congelados Los Jardines")}\n'
          F'BBQ congelados Pontezuela: {productos_sospechosos_por_super_mercado.count("BBQ congelados Pontezuela")}\n'
          F'Carniceria El Dorado: {productos_sospechosos_por_super_mercado.count("Carniceria El Dorado")}\n'
          F'Carniceria Depot: {productos_sospechosos_por_super_mercado.count("Carniceria Depot")}')


# Agregar una columna con el porcentage de diferencia entre cada precio con respecto a la mediana para observacion
def add_percentage(productos, mediana):
    porcentajes = []
    for index in range(len(productos)):
        prod = productos[index]
        for x in prod.index:
            porcentajes.append((prod.at[x, 'selprod_price'] / mediana[index]) * 100)
        prod['Porciento'] = porcentajes
        porcentajes = []


# Funcion para agregar que supermercado tiene productos sospechos.
def contar(market):
    productos_sospechosos_por_super_mercado.append(market)


# Agregar los productos sospechosos nuevos a la DB.
def add_data(data_a_salvar):
    date = datetime.date.today()
    if not data_a_salvar.empty:
        for index in data_a_salvar.index:
            query = F'INSERT INTO kpm VALUES ({data_a_salvar.at[index, "selprod_user_id"]}, {data_a_salvar.at[index, "selprod_product_id"]},' \
                    F'{data_a_salvar.at[index, "selprod_price"]}, 0, \'{date}\');'
            try:
                update(jeisonDB, query)
            except Exception:
                print(data_a_salvar)


# Actualizar el estado de un producto que ya existe y tiene un estado que hace discrepancia
def prod_arreglados(data):
    if not data.empty:
        for index in data.index:
            query = F'UPDATE kpm SET status = 2 WHERE selprod_user_id = {data.at[index, "selprod_user_id"]} AND selprod_product_id = ' \
                    F'{data.at[index, "selprod_product_id"]};'
            try:
                update(jeisonDB, query)
            except Exception:
                print(data)


# Productos que se volvieron a dañar con un precio muy por debajo que el resto
def desactivate_prod(data):
    if not data.empty:
        for index in data.index:
            query = F'UPDATE kpm SET status = 0 WHERE selprod_user_id = {data.at[index, "selprod_user_id"]} AND selprod_product_id = ' \
                    F'{data.at[index, "selprod_product_id"]};'
            try:
                update(jeisonDB, query)
            except Exception:
                print(data)


# Cargar data previa
def load_data():
    global preview_data
    query = 'SELECT * FROM kpm'
    preview_data = get_products(jeisonDB, query, 'none')


# Comparar cuales productos son nuevos en la lista de productos sospechosos
def compare_previews_data():
    global preview_data, precio_productos_super

    precio_productos_super = precio_productos_super[precio_productos_super['Porciento'] < 70]
    # Forzando un error de tipos con los valores, asegurando que todos sean int
    preview_data['selprod_product_id'] = preview_data['selprod_product_id'].astype(int)
    precio_productos_super['selprod_product_id'] = precio_productos_super['selprod_product_id'].astype(int)
    preview_data['selprod_user_id'] = preview_data['selprod_user_id'].astype(int)
    precio_productos_super['selprod_user_id'] = precio_productos_super['selprod_user_id'].astype(int)

    productos_malos_nuevos = preview_data.merge(precio_productos_super, on=['selprod_product_id', 'selprod_user_id'],
                                                how='right', indicator=True)
    desactivate_prod(productos_malos_nuevos[(productos_malos_nuevos['_merge'] == 'both') & (productos_malos_nuevos['status'] == '2')])
    productos_malos_nuevos = productos_malos_nuevos[productos_malos_nuevos['_merge'] == 'right_only']

    add_data(productos_malos_nuevos)

    productos_arreglados = preview_data.merge(precio_productos_super, on=['selprod_product_id', 'selprod_user_id'],
                                              how='left', indicator=True)
    productos_arreglados = productos_arreglados[(productos_arreglados['_merge'] == 'left_only') & (productos_arreglados['status'] == '1')]

    prod_arreglados(productos_arreglados)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    diff_value()
