from sqlalchemy import Column, Integer, String, Text, ARRAY, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from typing import List, Optional
from pydantic import BaseModel
from .database import Base

# SQLAlchemy Models

# Association table for mesh-tag many-to-many relationship
mesh_tag = Table(
    "mesh_tag",
    Base.metadata,
    Column("mesh_id", Integer, ForeignKey("meshes.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id"), primary_key=True),
)


class Mesh(Base):
    """SQLAlchemy model for 3D mesh metadata"""
    __tablename__ = "meshes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    vertices = Column(Integer)
    faces = Column(Integer)
    format = Column(String)
    preview_url = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    file_path = Column(String, nullable=True)
    
    # Relationships
    tags = relationship("Tag", secondary=mesh_tag, back_populates="meshes")


class Tag(Base):
    """SQLAlchemy model for mesh tags"""
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    
    # Relationships
    meshes = relationship("Mesh", secondary=mesh_tag, back_populates="tags")


# Pydantic Models (for API request/response)

class TagBase(BaseModel):
    name: str


class TagCreate(TagBase):
    pass


class TagResponse(TagBase):
    id: int

    class Config:
        orm_mode = True


class MeshBase(BaseModel):
    name: str
    vertices: int
    faces: int
    format: str
    preview_url: Optional[str] = None
    description: Optional[str] = None
    file_path: Optional[str] = None


class MeshCreate(MeshBase):
    tags: List[str] = []


class MeshUpdate(BaseModel):
    name: Optional[str] = None
    vertices: Optional[int] = None
    faces: Optional[int] = None
    format: Optional[str] = None
    preview_url: Optional[str] = None
    description: Optional[str] = None
    file_path: Optional[str] = None
    tags: Optional[List[str]] = None


class MeshResponse(MeshBase):
    id: int
    tags: List[TagResponse]

    class Config:
        orm_mode = True