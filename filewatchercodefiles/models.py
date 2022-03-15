from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

oldfilestoupload = Table(
    "old_file",
    Base.metadata,
    Column("file_size", Integer),
    Column("fileContents", String),
)

filestoupload = Table(
    "new_file",
    Base.metadata,
    Column("file_size", Integer),
    Column("publisher_id", Integer),
)

class files(Base):
    __tablename__ = "filesUplaoded"
    _id = Column(Integer, primary_key=True)
    filename = Column(String)

    books = relationship("Book", backref=backref("author"))
    publishers = relationship(
        "Publisher", secondary=author_publisher, back_populates="authors"
    )