 **基于Spring data relational对Spring data jdbc进行条件查询扩展** 


Spring jdbc配置类：

```
@Configuration
@EnableTransactionManagement
@EnableJdbcRepositories(basePackages = { "**" })
public class JdbcConfiguration extends AbstractJdbcConfiguration {

	@Override
	public DataAccessStrategySupport dataAccessStrategyBean(NamedParameterJdbcOperations operations,
			JdbcConverter jdbcConverter, JdbcMappingContext context, Dialect dialect) {
		return new SimpleDefaultDataAccessStrategy(dialect, new SqlGeneratorSource(context, jdbcConverter, dialect),
				context, jdbcConverter, operations);
	}

	@Override
	public JdbcEntityTemplate jdbcAggregateTemplate(ApplicationContext applicationContext,
			JdbcMappingContext mappingContext, JdbcConverter converter, DataAccessStrategy dataAccessStrategy) {
		return new JdbcEntityTemplate(applicationContext, mappingContext, converter,
				(DataAccessStrategySupport) dataAccessStrategy);
	}

}
```

实体类：

```
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

	@Transient
	@ManyToOne(property = "departmentId")
	private Department department;

}
```



```
@Table(value = "t_sys_department")
public class Department implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	private String name;
	
	@Transient
	@OneToMany(mappedBy = "department")
	private List<User> users;

}
```

dao类，@Query支持SpEL：
```
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface UserDao extends PagingAndSortingRepository<User, Long> {

	public List<User> findByUsername(String username);

	// entityName的值是Repository的domain class的@Table的value
	@Query(value = "select * from #{(#table==null || #table=='')?#entityName:#table} t where 1 = 1 #{#username != null?'and username like :username':''}")
	public List<User> queryByUsername(String table, String username);

	@Query(value = "select * from #{#entityName} t where 1 = 1 #{#username != null?'and username like :username':''}")
	public List<User> queryByUsername(String username);

	@Query(value = "select * from #{#params['table']} t where 1 = 1 #{#params['username'] != null?'and username like :username':''}")
	public List<User> findCondition(Map<String, Object> params);

	@Query(value = "select * from #{#table} t where 1 = 1 #{#params?.username != null?'and username like :username':''}")
	public List<User> findCondition(String table, User params);

	@Query(value = "select * from #{#entityName} t where 1 = 1 #{#params?.username != null?'and username like :username':''}")
	public List<User> findCondition(User params);

	@Query(value = "select * from #{#table} t where 1 = 1 and username like ?1")
	public List<User> selectByUsername(String table, String username);

	@Query(value = "select * from #{#table} t where 1 = 1 and username like :#{#username}")
	public List<User> fetchByUsername(String table, String username);

	@Query(value = "select * from #{#table} t where 1 = 1 and username like :username")
	public List<User> fetchByUsername1(String table, String username);

	@Query(value = "select * from #{#entityName} t where 1 = 1 #{#params?.username != null?'and username like :#{#params.username}':''}")
	public List<User> fetchCondition(User params);

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
	
}
```

