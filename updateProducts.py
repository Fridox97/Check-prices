from SSH import update, get_products
from DBclass import DbInfo

jeisonDB = DbInfo('localhost', 'jeison', 'jdiazsuperkiwi21j', 'suspiciusPrices', 3306, 'ec2-18-206-137-112.compute-1.amazonaws.com', 'ubuntu', 22,
                  './JeisonDiaz.pem')
kiwiDB = DbInfo('localhost', 'noelUsr', 'TG#2a12$', 'db_noelgrcr', 3306, 'ec2-18-225-14-113.us-east-2.compute.amazonaws.com', 'ubuntu', 22,
                './noelgrowcer.pem')

def activateProducts():
    productsToActivate = get_products(jeisonDB, 'SELECT * FROM kpm WHERE status = 2', 'none').values.tolist()
    cont = 0
    for product in productsToActivate:
        shopId = product[0]
        productId = product[1]
        query = 'UPDATE tbl_seller_products SET selprod_active = 1 WHERE selprod_user_id = ' + str(shopId) + ' AND selprod_product_id = ' + str(
            productId) + ';'
        cont += 1
        update(kiwiDB, query)
    print('Se han activado ', cont, ' productos')

def desactivateProducts():
    productsToActivate = get_products(jeisonDB, 'SELECT * FROM kpm WHERE status = 0', 'none').values.tolist()
    cont = 0
    for product in productsToActivate:
        shopId = product[0]
        productId = product[1]
        query = 'UPDATE tbl_seller_products SET selprod_active = 0 WHERE selprod_user_id = ' + str(shopId) + ' AND selprod_product_id = ' + str(
            productId) + ';'
        update(kiwiDB, query)

        query = 'UPDATE kpm SET status = 1 WHERE selprod_user_id = ' + str(
            shopId) + ' AND selprod_product_id = ' + str(
            productId) + ';'
        update(jeisonDB, query)
        cont += 1
    print('Se han desactivado ', cont, ' productos')

