 **基于Spring data relational对Spring data jdbc进行条件查询扩展** 


Spring jdbc配置类：

```
@Configuration
@EnableTransactionManagement
@EnableJdbcRepositories(basePackages = { "**" })
public class JdbcConfiguration extends AbstractJdbcConfiguration {

	@Bean
	@Override
	public DataAccessStrategySupport dataAccessStrategyBean(NamedParameterJdbcOperations operations,
			JdbcConverter jdbcConverter, JdbcMappingContext context, Dialect dialect) {
		return new SimpleDefaultDataAccessStrategy(dialect, new SqlGeneratorSource(context, jdbcConverter, dialect),
				context, jdbcConverter, operations);
	}

	@Override
	public JdbcAggregatePlusTemplate jdbcAggregateTemplate(ApplicationContext applicationContext,
			JdbcMappingContext mappingContext, JdbcConverter converter, DataAccessStrategy dataAccessStrategy) {
		return new JdbcAggregatePlusTemplate(applicationContext, mappingContext, converter,
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

}
```

dao类，@Query支持SpEl：
```
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface UserDao extends PagingAndSortingRepository<User, Long> {

	public List<User> findByUsername();

	@Query(value = "select * from #{#table} t where 1 = 1 #{#username != null?'and username like :username':''}")
	public List<User> queryByUsername(String table, String username);

	@Query(value = "select * from #{#params['table']} t where 1 = 1 #{#params['username'] != null?'and username like :username':''}")
	public List<User> findCondition(Map<String, Object> params);

	@Query(value = "select * from #{#table} t where 1 = 1 #{#params?.username != null?'and username like :username':''}")
	public List<User> findCondition(String table, User params);

	@Query(value = "select * from t_sys_user t where 1 = 1 #{#params?.username != null?'and username like :username':''}")
	public List<User> findCondition(User params);

}
```


测试类：
```
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;

@SpringBootTest
public class JdbcTest {

    @Autowired
	private JdbcAggregatePlusTemplate jdbcAggregatePlusTemplate;

	@Test
	public void test() {
		try {
			Integer pageSize = 10;
			Integer pageNum = 1;
			Criteria criteria = Criteria.where("name").like("%aa%").and("department.name").is("某某部门");
			Query query = Query.query(criteria).with(PageRequest.of(pageNum - 1, pageSize));
			Page<User> pageObject = jdbcAggregatePlusTemplate.findPage(query, User.class);

			Long total = pageObject.getTotalElements();
			List<User> rows = pageObject.getContent();

			rows.forEach(row -> System.out.println(row.getUsername()));

			System.out.println(total);
			System.out.println("end");
		} catch (Exception | Error e) {
			e.printStackTrace();
		}
	}
}
```

