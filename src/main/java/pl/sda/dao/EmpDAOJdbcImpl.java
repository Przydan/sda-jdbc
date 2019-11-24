package pl.sda.dao;

import pl.sda.domain.Employee;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

/**
 * Created by pzawa on 02.02.2017.
 */
public class EmpDAOJdbcImpl implements EmpDAO {

    private static final String QUERY_BY_ID = "SELECT * FROM Emp WHERE empno = ?";
    private static final String INSERT_STMT = "INSERT INTO" +
            " Emp(empno, ename, job, manager, hiredate, salary, commision, deptno) " +
            "VALUES (?,?,?,?,?,?,?,?)";
    private static final String UPDATE_STMT = "UPDATE " +
            "Emp set ename = ?, job = ?, manager = ?, hiredate = ?, salary = ?, commision = ?, deptno = ? " +
            "WHERE empno = ?";
    private static final String DELETE_STMT = "DELETE FROM Emp WHERE empno = ?";
    private static final String MULTI_INSERT_STMT = "INSERT INTO" +
            " Emp(empno, ename, job, manager, hiredate, salary, commision, deptno) " +
            "VALUES (?,?,?,?,?,?,?,?)";
    ;


    private final JdbcConnectionManager jdbcConnectionManager;

    public EmpDAOJdbcImpl(JdbcConnectionManager jdbcConnectionManager) {
        this.jdbcConnectionManager = jdbcConnectionManager;
    }

    @Override
    public Employee findById(int id) throws Exception {

        try (Connection conn = jdbcConnectionManager.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(QUERY_BY_ID);
            ps.setInt(1, id);

            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                Employee employee = mapFromResultSet(rs);
                return employee;
            }
        }

        return null;
    }

    private Employee mapFromResultSet(ResultSet rs) throws SQLException {
        int empno = rs.getInt("empno");
        String ename = rs.getString("ename");
        String job = rs.getString("job");
        int manager = rs.getInt("manager");
        Date hiredate = rs.getDate("hiredate");
        BigDecimal salary = rs.getBigDecimal("salary");
        BigDecimal commision = rs.getBigDecimal("commision");
        int deptno = rs.getInt("deptno");

        return new Employee(empno, ename, job, manager, hiredate, salary, commision, deptno);
    }

    @Override
    public void create(Employee employee) throws Exception {
        try (Connection conn = jdbcConnectionManager.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(INSERT_STMT);

            ps.setInt(1, employee.getEmpno());
            ps.setString(2, employee.getEname());
            ps.setString(3, employee.getJob());
            ps.setInt(4, employee.getManager());
            ps.setDate(5, new Date(employee.getHiredate().getTime()));
            ps.setBigDecimal(6, employee.getSalary());
            ps.setBigDecimal(7, employee.getCommision());
            ps.setInt(8, employee.getDeptno());

            int numberOfAffectedRows = ps.executeUpdate();

            System.out.println("EmpDAO.create() number of affected rows: " + numberOfAffectedRows);
        }

    }

    @Override
    public void update(Employee employee) throws Exception {
        try (Connection conn = jdbcConnectionManager.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(UPDATE_STMT);

            ps.setString(1, employee.getEname());
            ps.setString(2, employee.getJob());
            ps.setInt(3, employee.getManager());
            ps.setDate(4, new Date(employee.getHiredate().getTime()));
            ps.setBigDecimal(5, employee.getSalary());
            ps.setBigDecimal(6, employee.getCommision());
            ps.setInt(7, employee.getDeptno());
            ps.setInt(8, employee.getEmpno());

            int numberOfAffectedRows = ps.executeUpdate();
            System.out.println("EmpDAO.update() number of affected rows: " + numberOfAffectedRows);
        }
    }

    @Override
    public void delete(int id) throws Exception {
        try (Connection conn = jdbcConnectionManager.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(DELETE_STMT);
            ps.setInt(1, id);

            int numberOfAffectedRows = ps.executeUpdate();

            System.out.println("EmpDAO.delete() number of affected rows: " + numberOfAffectedRows);
        }
    }

    // tworzenie listy powinnno być tranzakcyjne

    @Override
    public void create(List<Employee> employees) throws Exception {
        try (Connection conn = jdbcConnectionManager.getConnection()) {
            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement(INSERT_STMT);

            for (Employee employee : employees) {
                ps.setInt(1, employee.getEmpno());
                ps.setString(2, employee.getEname());
                ps.setString(3, employee.getJob());
                ps.setInt(4, employee.getManager());
                ps.setDate(5, new Date(employee.getHiredate().getTime()));
                ps.setBigDecimal(6, employee.getSalary());
                ps.setBigDecimal(7, employee.getCommision());
                ps.setInt(8, employee.getDeptno());
                ps.executeUpdate();
            }
            conn.commit();
        }
    }


    private static final String QUERY_SALARY_BY_DEPT = "select sum(salary) from emp where deptno = ?";

    @Override
    public BigDecimal getTotalSalaryByDept(int dept) throws Exception {
        try (Connection conn = jdbcConnectionManager.getConnection()) {

            PreparedStatement ps = conn.prepareStatement(QUERY_SALARY_BY_DEPT);
            ps.setInt(1, dept);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getBigDecimal(1);
            }
        }

        // Lepiej korzystać z Optional
        return null;
    }
}
