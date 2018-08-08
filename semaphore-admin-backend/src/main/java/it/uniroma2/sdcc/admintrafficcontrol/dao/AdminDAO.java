package it.uniroma2.sdcc.admintrafficcontrol.dao;

import it.uniroma2.sdcc.admintrafficcontrol.entity.Admin;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;


public interface AdminDAO extends JpaRepository<Admin, Long> {

    @Query("SELECT a FROM Admin a WHERE a.email = :email")
    Admin findByEmail(@Param("email") String email);

    @Query("SELECT a FROM Admin a WHERE a.username = :username")
    Admin findByUsername(@Param("username") String username);


}
