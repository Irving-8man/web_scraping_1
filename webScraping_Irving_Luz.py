from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import sqlite3
import math
import re
from concurrent.futures import ThreadPoolExecutor
import time

# Diccionario para estrellas
estrellas_dict = {
    'One': 1,
    'Two': 2,
    'Three': 3,
    'Four': 4,
    'Five': 5
}

# Crear tablas (usado solo una vez al inicio)
def crear_tablas():
    conn = sqlite3.connect('libros_secciones.db')
    cursor = conn.cursor()
    
    # Tabla de secciones
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS secciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        url TEXT NOT NULL,
        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Tabla de libros
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS libros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_seccion INTEGER,
        nombre TEXT NOT NULL,
        enlace TEXT NOT NULL,
        estrellas INTEGER,
        precio REAL,
        FOREIGN KEY (id_seccion) REFERENCES secciones(id)
    )
    ''')

    # Tabla para características de libros
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS caracteristicasLibros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_libro INTEGER,
        descripcion TEXT,
        UPC TEXT,
        tipoProducto TEXT,
        precioSinImpu REAL,
        precioConImpu REAL,
        impuesto REAL,
        disponibilidad TEXT,
        numReviews INTEGER,
        FOREIGN KEY (id_libro) REFERENCES libros(id)
    )
    ''')
    conn.commit()
    conn.close()


# Obtener URLs de las secciones y almacenarlas en la base de datos
def obtener_urls_secciones(url_base):
    conn = sqlite3.connect('libros_secciones.db')
    cursor = conn.cursor()
    response = requests.get(url_base)
    soup = BeautifulSoup(response.text, 'html.parser')
    secciones = soup.select('.side_categories > ul.nav.nav-list > li > ul > li > a')
    
    urls_secciones = []
    for sec in secciones:
        nombre_seccion = sec.text.strip()
        url_seccion = urljoin(url_base, sec['href'])
        cursor.execute('INSERT INTO secciones (nombre, url) VALUES (?, ?)', (nombre_seccion, url_seccion))
        conn.commit()
        urls_secciones.append((nombre_seccion, url_seccion))
    
    conn.close()
    return urls_secciones




# Obtener URLs de las páginas de una sección
def obtener_urls_paginas_seccion(url_seccion):
    response = requests.get(url_seccion)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    num_libros_element = soup.select_one('.form-horizontal > strong')
    if num_libros_element:
        num_libros = int(num_libros_element.text.strip())
        if num_libros <= 20:
            return [url_seccion]
        else:
            paginas = math.ceil(num_libros / 20)
            return [url_seccion.replace('index.html', f'page-{i}.html') for i in range(1, paginas + 1)]
    else:
        return []




# Obtener características de un libro
def obtener_caracteristicas_libro(url_libro, id_libro, conn):
    cursor = conn.cursor()
    response = requests.get(url_libro)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    descripcion = soup.select_one('div.sub-header + p')
    descripcion_texto = descripcion.text.strip() if descripcion else None

    tabla = soup.select_one('table.table.table-striped')
    if tabla:
        datos = {}
        for fila in tabla.find_all('tr'):
            encabezado = fila.find('th').text.strip()
            valor = fila.find('td').text.strip()
            datos[encabezado] = valor

        UPC = datos.get('UPC', None)
        tipoProducto = datos.get('Product Type', None)
        precioSinImpu = float(re.sub(r'[^\d.]', '', datos.get('Price (excl. tax)', '0')))
        precioConImpu = float(re.sub(r'[^\d.]', '', datos.get('Price (incl. tax)', '0')))
        impuesto = float(re.sub(r'[^\d.]', '', datos.get('Tax', '0')))
        disponibilidad = datos.get('Availability', None)
        numReviews = int(datos.get('Number of reviews', '0'))

        cursor.execute('''
        INSERT INTO caracteristicasLibros (
            id_libro, descripcion, UPC, tipoProducto, precioSinImpu, 
            precioConImpu, impuesto, disponibilidad, numReviews
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id_libro, descripcion_texto, UPC, tipoProducto, precioSinImpu, 
              precioConImpu, impuesto, disponibilidad, numReviews))
        conn.commit()




# Obtener libros de una página y almacenarlos en la base de datos
def obtener_libros_de_pagina(url_pagina, id_seccion, conn):
    cursor = conn.cursor()
    response = requests.get(url_pagina)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    productos = soup.select('section > div > ol.row > li > article.product_pod')
    
    for producto in productos:
        estrellas_clase = producto.select_one('p.star-rating')['class'][1]
        estrellas = estrellas_dict.get(estrellas_clase, 0)
        enlace_relativo = producto.select_one('h3 > a')['href']
        titulo = producto.select_one('h3 > a')['title']
        
        if enlace_relativo.startswith('/') or enlace_relativo.startswith('..'):
            enlace_completo = 'https://books.toscrape.com/catalogue/' + enlace_relativo.lstrip('/').replace('../', '')
        else:
            enlace_completo = urljoin('https://books.toscrape.com/catalogue/', enlace_relativo)
    
        precio_str = producto.select_one('div.product_price > p.price_color').text.strip()
        precio_str = re.sub(r'[^\d.,]', '', precio_str).replace(',', '')

        try:
            precio = float(precio_str)
        except ValueError:
            precio = 0.0

        cursor.execute('''
        INSERT INTO libros (id_seccion, nombre, enlace, estrellas, precio) 
        VALUES (?, ?, ?, ?, ?)
        ''', (id_seccion, titulo, enlace_completo, estrellas, precio))
        conn.commit()

        cursor.execute('SELECT id FROM libros WHERE enlace = ?', (enlace_completo,))
        id_libro = cursor.fetchone()[0]
        obtener_caracteristicas_libro(enlace_completo, id_libro, conn)



# Procesar libros de una sección
def procesar_libros_seccion(id_seccion, url_seccion):
    conn = sqlite3.connect('libros_secciones.db')
    cursor = conn.cursor()
    
    urls_paginas = obtener_urls_paginas_seccion(url_seccion)
    for url_pagina in urls_paginas:
        obtener_libros_de_pagina(url_pagina, id_seccion, conn)
    
    conn.close()



# Procesar todas las secciones
def procesar_secciones(secciones):
    with ThreadPoolExecutor() as executor:
        for nombre, url in secciones:
            cursor = sqlite3.connect('libros_secciones.db').cursor()
            cursor.execute('SELECT id FROM secciones WHERE nombre = ?', (nombre,))
            id_seccion = cursor.fetchone()[0]
            cursor.close()
            
            executor.submit(procesar_libros_seccion, id_seccion, url)


# Ejecución principal
def main():
    start_time = time.time()
    crear_tablas()
    url_base = 'https://books.toscrape.com/index.html'
    secciones = obtener_urls_secciones(url_base)
    procesar_secciones(secciones)
    # Tiempo total del programa
    print(f"Tiempo total de ejecución: {time.time() - start_time:.2f} segundos")


# Ejecutar el script
if __name__ == '__main__':
    main()


"""
Examen por :
    Irving Cupul Uc
    Luz García Peña
"""