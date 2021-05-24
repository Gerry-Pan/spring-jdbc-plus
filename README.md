 **基于Spring data relational对Spring data jdbc进行条件查询扩展** 


Spring jdbc配置类：

```
@Configuration
@EnableTransactionManagement
@EnableJdbcRepositories(basePackages = { "**" })
public class JdbcConfiguration extends AbstractJdbcConfiguration {

	@Autowired
	private NamedParameterJdbcOperations operations;

	@Override
	public JdbcEntityTemplate jdbcAggregateTemplate(ApplicationContext applicationContext,
			JdbcMappingContext mappingContext, JdbcConverter converter, DataAccessStrategy dataAccessStrategy) {
		return new JdbcEntityTemplate(applicationContext, mappingContext, converter, dataAccessStrategy,
				jdbcDialect(operations), operations);
	}

}
```

实体类：

```
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

@Table(value = "t_sys_user")
public class User implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	private String username;

	private String password;

	private Long departmentId;

	@ManyToOne(property = "departmentId")
	private Department department;

}
```



```
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

@Table(value = "t_sys_department")
public class Department implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	private String name;
	
	@OneToMany(mappedBy = "department")
	private List<User> users;

}
```

dao类，@Query支持SpEL：
```
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface UserDao extends PagingAndSortingRepository<User, Long> {

	public Long countByUsernameLike(String username);

	public Boolean existsByDepartmentNameLike(String name);

	public List<User> findByUsernameLike(String username);

	public List<User> findByDepartmentNameLike(String name);

	public List<User> findByRolesNameLike(String name);

	public Page<User> findByDepartmentNameLike(String name, Pageable pageable);

	public Page<User> findByDepartmentNameLikeOrderByUsername(String name, Pageable pageable);

	public Page<User> findByDepartmentNameLikeOrderByDepartmentName(String name, Pageable pageable);

	public Page<User> findByDepartmentNameLikeOrderByUsernameAscDepartmentNameAsc(String name, Pageable pageable);

	public Page<User> findByRolesNameLike(String name, Pageable pageable);

	public Page<User> findByUsernameLike(String username, Pageable pageable);

	public Slice<User> searchByUsernameLike(String username, Pageable pageable);

	/**
	 * 1.优先解析SQL中的<b style="color:
	 * red;">带冒号SpEL表达式</b>，例如:#{#username}，将其替换为特定参数名称，例如":__$synthetic$__0"<br />
	 * 2.然后再对整条SQL的其他SpEL进行解析，SpEL支持三目运算符<br />
	 * <br />
	 * #{#_entity}会被转换成Repository的domain class的所有column<br />
	 * #{#entityName}会被转换成Repository的domain class的@Table的value<br />
	 * <br />
	 * 进行SpEL解析后的SQL是<br />
	 * select id, username, password, department_id from "t_sys_user" t where 1 = 1
	 * and username like :__$synthetic$__0<br />
	 * <br />
	 * 最终的SQL是<br />
	 * select id, username, password, department_id from "t_sys_user" t where 1 = 1
	 * and username like ?
	 * 
	 * @param username
	 * @return
	 */
	@Query(value = "select #{#_entity} from #{#entityName} t where 1 = 1 and t.username like :#{#username}")
	public List<User> readByUsername(String username);

	/**
	 * #{#entityName}会被转换成Repository的domain class的@Table的value
	 * 
	 * @param table
	 * @param username
	 * @return
	 */
	@Query(value = "select #{#_entity} from #{(#table==null || #table=='')?#entityName:#table} t where 1 = 1 #{#username != null?'and username like :username':''}")
	public List<User> queryByUsername(String table, String username);

	@Query(value = "select #{#_entity} from #{#entityName} t where 1 = 1 #{#username != null?'and username like :username':''}")
	public List<User> queryByUsername(String username);

	@Query(value = "select #{#_entity} from #{#params['table']} t where 1 = 1 #{#params['username'] != null?'and username like :#{#params['username']}':''}")
	public List<User> findCondition(Map<String, Object> params);

	@Query(value = "select #{#_entity} from #{#table} t where 1 = 1 #{#params?.username != null?'and username like :#{#params.username}':''}")
	public List<User> findCondition(String table, User params);

	@Query(value = "select #{#_entity} from #{#entityName} t where 1 = 1 #{#params?.username != null?'and username like :#{#params.username}':''}")
	public List<User> findCondition(User params);

	@Query(value = "select #{#_entity} from #{#table} t where 1 = 1 and username like ?1")
	public List<User> selectByUsername(String table, String username);

	@Query(value = "select #{#_entity} from #{#table} t where 1 = 1 and username like :#{#username}")
	public List<User> fetchByUsername(String table, String username);

	@Query(value = "select #{#_entity} from #{#table} t where 1 = 1 and username like :username")
	public List<User> searchByUsername(String table, String username);

	// 0 is parameter index of method
	@Query(value = "select #{#_entity} from #{#entityName} t where 1 = 1 #{[0]?.username != null?'and username like :#{[0].username}':''} #{[0]?.departmentId != null?'and department_id = :#{[0].departmentId}':''}")
	public List<User> fetchCondition(User params);

	@Query(value = "select #{#_entity} from #{#params['table']} t where 1 = 1 #{[0]['username'] != null?'and username like :#{[0]['username']}':''}")
	public List<User> fetchCondition(Map<String, Object> params);

	@Query(value = "select count(1) from #{#params['table']} t where 1 = 1 #{[0]['username'] != null?'and username like :#{[0]['username']}':''}")
	public Long countCondition(Map<String, Object> params);

}
```


测试类：
```
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;

@SpringBootTest
public class JdbcTest {

	@Autowired
	private UserDao userDao;
	
	@Autowired
	private JdbcEntityTemplate jdbcEntityTemplate;

	@Test
	public void test() {
		try {
			Integer pageSize = 10;
			Integer pageNum = 1;
			Criteria criteria = Criteria.where("name").like("%aa%").and("department.name").is("某某部门");
			Query query = Query.query(criteria).with(PageRequest.of(pageNum - 1, pageSize));
			Page<User> pageObject = jdbcEntityTemplate.findPage(query, User.class);

			Long total = pageObject.getTotalElements();
			List<User> rows = pageObject.getContent();

			rows.forEach(row -> System.out.println(row.getUsername()));

			System.out.println(total);
			System.out.println("end");
		} catch (Exception | Error e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 分表时使用方式
	 */
	@Test
	public void test1() {
		try {
			Integer pageSize = 10;
			Integer pageNum = 1;
			Criteria criteria = Criteria.where("name").like("%aa%").and("department.name").is("某某部门");
			Query query = Query.query(criteria).table("t_sys_user_20210101")
					.with(PageRequest.of(pageNum - 1, pageSize));
			Page<User> pageObject = jdbcEntityTemplate.findPage(query, User.class);

			Long total = pageObject.getTotalElements();
			List<User> rows = pageObject.getContent();

			rows.forEach(row -> System.out.println(row.getUsername()));

			System.out.println(total);
			System.out.println("end");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test2() {
		try {
			Integer pageSize = 10;
			Integer pageNum = 1;
			Criteria criteria = Criteria.where("users.username").like("%aa%");
			Query query = Query.query(criteria).with(PageRequest.of(pageNum - 1, pageSize));
			Page<Department> pageObject = jdbcEntityTemplate.findPage(query, Department.class);

			Long total = pageObject.getTotalElements();
			List<Department> rows = pageObject.getContent();

			rows.forEach(row -> System.out.println(row.getName()));

			System.out.println(total);
			System.out.println("end");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 分表时使用方式
	 */
	@Test
	public void test3() {
		try {
			User user = new User().setUsername("%a%");
			List<User> rows = userDao.findCondition("t_sys_user_20210101", user);
			rows.forEach(row -> System.out.println(row.getUsername()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test4() {
		try {
			User user = new User().setUsername("%a%");
			List<User> rows = userDao.findCondition(user);
			rows.forEach(row -> System.out.println(row.getUsername()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test5() {
		try {
			Integer pageSize = 10;
			Integer pageNum = 1;

			SubQuery subQuery = SubQuery.builder().table(User.class).columns(new String[] { "id" })
					.localKey("departmentId").inverseKey("id").criteria(Criteria.where("username").like("%a%")).build();

			Criteria criteria = Criteria.where("name").is("部门").and(subQuery.exists());
			Query query = Query.query(criteria).with(PageRequest.of(pageNum - 1, pageSize));
			List<Department> rows = jdbcEntityTemplate.findList(query, Department.class);

			rows.forEach(row -> System.out.println(row.getName()));

			System.out.println("end");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test6() {
		try {
			Integer pageSize = 10;
			Integer pageNum = 1;

			SubQuery subQuery = SubQuery.builder().table(User.class).columns(new String[] { "id" })
					.localKey("departmentId").inverseKey("id").criteria(Criteria.where("username").like("%a%")).build();

			Criteria criteria = Criteria.where("name").is("部门").and(subQuery.exists());
			Query query = Query.query(criteria).with(PageRequest.of(pageNum - 1, pageSize));
			Page<Department> pageObject = jdbcEntityTemplate.findPage(query, Department.class);

			Long total = pageObject.getTotalElements();
			List<Department> rows = pageObject.getContent();

			rows.forEach(row -> System.out.println(row.getName()));

			System.out.println(total);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test6() {
		try {
			System.out.println(userDao.existsByDepartmentNameLike("%部门%"));
			System.out.println("end");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
```

