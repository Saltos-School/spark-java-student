package org.saltos.school.spark;

import java.io.Serializable;
import java.util.Objects;

public class PersonaBean implements Serializable {

    private String nombre;

    private Long edad;

    public Long getEdad() {
        return edad;
    }

    public void setEdad(Long edad) {
        this.edad = edad;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonaBean that = (PersonaBean) o;
        return Objects.equals(nombre, that.nombre) && Objects.equals(edad, that.edad);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nombre, edad);
    }
}
