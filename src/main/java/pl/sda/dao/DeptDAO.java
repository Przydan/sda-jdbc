package pl.sda.dao;

import pl.sda.domain.Department;

import java.util.List;

/**
 * Created by pzawa on 02.02.2017.
 */
public interface DeptDAO {
    Department findById(int id) throws Exception;

    void create(Department department) throws Exception;

    void update(Department department) throws Exception;

    void delete(int id) throws Exception;

    void create(List<Department> employees) throws Exception;
}
