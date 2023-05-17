import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import configparser
from tabulate import tabulate

Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __str__(self):
        return f'{self.id}: {self.name}'

class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), unique=True)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="book")

    def __str__(self):
        return f'{self.id}: ({self.title}, {self.publisher})'
    
class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __str__(self):
        return f'{self.id}: {self.name}'
    
class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer)

    book = relationship(Book, backref="stock")
    shop = relationship(Shop, backref="stock")

    def __str__(self):
        return f'{self.id}: ({self.book}, {self.shop}, {self.count})'
    
class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Numeric, nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer)

    stock = relationship(Stock, backref="sale")

    def __str__(self):
        return f'{self.id}: ({self.price}, {self.data_sale}, {self.stock}, {self.count})'
    
def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

config = configparser.ConfigParser()  # создаём объекта парсера
config.read("settings.ini")  # читаем конфиг
host = config['my_DB']['DB_HOST']
port = str(config['my_DB']['DB_PORT'])
db_name = config['my_DB']['DB_NAME']
user = config['my_DB']['DB_USER']
password = config['my_DB']['DB_PASSWORD']

DSN = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

# сессия
Session = sessionmaker(bind=engine)
session = Session()

# создание объектов
pub1 = Publisher(name="Dan Millman")
pub2 = Publisher(name="Robin Sharma")
pub3 = Publisher(name="Frederick Backman")
book1 = Book(title = "Way of the Peacefull Warrior", publisher = pub1)
book2 = Book(title = "The laws of spirit", publisher = pub1)
book3 = Book(title = "Saint, serfinger and CEO", publisher = pub2)
book4 = Book(title = "I am the best", publisher = pub2)
book5 = Book(title = "A Man Called Ove", publisher = pub3)
book6 = Book(title = "Beartown", publisher = pub3)
shop1 = Shop(name="Буквоед")
shop2 = Shop(name="Лабиринт")
shop3 = Shop(name="Калевала")
stock1 = Stock(book = book1, shop = shop1, count = 5)
stock2 = Stock(book = book2, shop = shop2, count = 10)
stock3 = Stock(book = book3, shop = shop1, count = 15)
stock4 = Stock(book = book4, shop = shop3, count = 8)
stock5 = Stock(book = book5, shop = shop3, count = 12)
stock6 = Stock(book = book6, shop = shop1, count = 25)
sale1 = Sale(price = 625.6, date_sale = '2023-05-12', stock = stock2, count = 4)
sale2 = Sale(price = 335.6, date_sale = '2023-05-10', stock = stock1, count = 3)
sale3 = Sale(price = 555, date_sale = '2023-05-09', stock = stock3, count = 1)
sale4 = Sale(price = 700, date_sale = '2023-05-11', stock = stock4, count = 2)
sale5 = Sale(price = 870, date_sale = '2023-04-22', stock = stock5, count = 3)
sale6 = Sale(price = 580, date_sale = '2023-04-13', stock = stock6, count = 2)

session.add_all([pub1, pub2, pub3, book1, book2, book3, book4, book5, book6, shop1, shop2, shop3, stock1, stock2, stock3, stock4, stock5, stock6, sale1, sale2, sale3, sale4, sale5, sale6])
session.commit()  # фиксируем изменения

def publisher_books_sales(autor):
    data = []

    subq = session.query(Publisher.id).filter(Publisher.name == autor).subquery()
    autors_books = session.query(Book).join(subq, Book.id_publisher == subq.c.id)
    for autors_book in autors_books.all():        
        book_shops = session.query(Shop).join(Stock).filter(Stock.id_book == autors_book.id)
        for book_shop in book_shops.all():
            prices_books = session.query(Sale).join(Stock).filter(Stock.id == autors_book.id)
            for books_price in prices_books.all():
                # print(f'{autors_book.title} | {book_shop.name} | {books_price.price} | {books_price.date_sale}')               
                data.append([autors_book.title, book_shop.name, books_price.price, books_price.date_sale])

    headers = ["Title", "Shop", "Price", "Date"]
    print(tabulate(data, headers, tablefmt="pipe"))

session.close()

autor = input("Введите имя или id автора: ")
publisher_books_sales(autor)
