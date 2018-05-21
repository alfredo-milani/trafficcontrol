package it.uniroma2.sdcc.admintrafficcontrol.entity;

import it.uniroma2.sdcc.admintrafficcontrol.constants.Tables;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name = Tables.T_ADMIN.ADMIN)
public class Admin {

    @Id
    @Column(name = Tables.T_ADMIN.C_ID)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = Tables.T_ADMIN.C_EMAIL, unique = true)
    @NotNull
    private String email;
    @Column(name = Tables.T_ADMIN.C_USERNAME)
    private String username;
    @Column(name = Tables.T_ADMIN.C_PASSWORD)
    @NotNull
    private String password;

    @Transient
    private boolean login;

    public Admin(String email, String username, String password) {
        this.email = email;
        this.username = username;
        this.password = password;
    }

    public void update(@NotNull Admin newAdmin) {
        this.email = newAdmin.email;
        this.username = newAdmin.username;
        this.password = newAdmin.password;
    }

    public boolean isLogin() {
        return login;
    }

    public void setLogin(boolean login) {
        this.login = login;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return String.format(
                "User: %s, %s, %s.",
                email,
                username,
                login
        );
    }

}
