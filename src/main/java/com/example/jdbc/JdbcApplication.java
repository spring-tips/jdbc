package com.example.jdbc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.simpleflatmapper.jdbc.spring.JdbcTemplateMapperFactory;
import org.simpleflatmapper.jdbc.spring.ResultSetExtractorImpl;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.event.BeforeSaveEvent;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.jdbc.mapping.model.NamingStrategy;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

@SpringBootApplication
@Log4j2
public class JdbcApplication {

		@Bean
		JdbcTemplate jdbcTemplate(DataSource dataSource) {
				return new JdbcTemplate(dataSource);
		}

		@Bean
		PlatformTransactionManager dataSourceTransactionManager(DataSource dataSource) {
				return new DataSourceTransactionManager(dataSource);
		}

		public static void main(String[] args) {
				SpringApplication.run(JdbcApplication.class, args);
		}
}

@Log4j2
@Order(1)
@Component
class QueryCustomersAndOrdersCount implements ApplicationRunner {

		private final JdbcTemplate jdbcTemplate;

		QueryCustomersAndOrdersCount(JdbcTemplate jdbcTemplate) {
				this.jdbcTemplate = jdbcTemplate;
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {
				StringUtils.line();
				List<CustomerResult> results = this.jdbcTemplate.query("select c.*, (select count(o.id)  from orders o where o.customer_fk = c.id) as count from customers c;",
					(resultSet, i) -> new CustomerResult(resultSet.getLong("id"), resultSet.getString("name"), resultSet.getString("email"), resultSet.getInt("count")));
				results.forEach(log::info);
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class CustomerResult {
				private Long id;
				private String name, email;
				private int orderCount;
		}
}

@Log4j2
@Order(2)
@Component
class QueryCustomersAndOrders implements ApplicationRunner {

		private final JdbcTemplate jdbcTemplate;

		QueryCustomersAndOrders(JdbcTemplate jdbcTemplate) {
				this.jdbcTemplate = jdbcTemplate;
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {
				StringUtils.line();
				ResultSetExtractor<Collection<Customer>> customerResultSetExtractor = rs -> {

						Map<Long, Customer> customerMap = new HashMap<>();

						Function<ResultSet, Customer> mapCustomer = incoming -> {
								try {
										return new Customer(rs.getLong("cid"), rs.getString("name"), rs.getString("email"), new HashSet<>());
								}
								catch (SQLException e) {
										throw new RuntimeException(e);
								}
						};
						Function<ResultSet, Order> mapOrder = incoming -> {
								try {
										return new Order(rs.getLong("oid"), rs.getString("sku"));
								}
								catch (SQLException e) {
										throw new RuntimeException(e);
								}
						};
						Customer currentCustomer = null;
						while (rs.next()) {
								long id = rs.getLong("cid");
								if (currentCustomer == null || currentCustomer.getId() != id) {
										currentCustomer = mapCustomer.apply(rs);
								}
								currentCustomer.getOrders().add(mapOrder.apply(rs));
								customerMap.put(currentCustomer.getId(), currentCustomer); // this will be called several times with the same key but.. meh
						}
						return customerMap.values();
				};

				Collection<Customer> josh = this.jdbcTemplate.query("select c.id as cid, c.* , o.id as oid, o.* from customers c left join orders o on c.id = o.customer_fk order by cid ", customerResultSetExtractor);
				josh.forEach(log::info);
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Order {
				private Long id;
				private String sku;
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String name, email;
				private Set<Order> orders = new HashSet<>();
		}
}

@Order(3)
@Log4j2
@Component
class QueryCustomersAndOrdersSimpleFlatMapper implements ApplicationRunner {

		QueryCustomersAndOrdersSimpleFlatMapper(JdbcTemplate jdbcTemplate) {
				this.jdbcTemplate = jdbcTemplate;
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String name, email;
				private Set<Order> orders = new HashSet<>();
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Order {
				private Long id;
				private String sku;
		}

		private final JdbcTemplate jdbcTemplate;

		@Override
		public void run(ApplicationArguments args) throws Exception {

				StringUtils.line();

				ResultSetExtractorImpl<Customer> customerResultSetExtractor = JdbcTemplateMapperFactory
					.newInstance()
					.addKeys("id")
					.newResultSetExtractor(Customer.class);

				Collection<Customer> josh = jdbcTemplate.query("select c.id as id, c.name as name, c.email as email , o.id as orders_id, o.sku as orders_sku from customers c left join orders o on c.id=o.customer_fk  ", customerResultSetExtractor);
				josh.forEach(log::info);
		}
}

@Order(4)
@Log4j2
@Configuration
class JdbcTemplateCustomerServiceWriter implements ApplicationRunner {

		JdbcTemplateCustomerServiceWriter(CustomerService customerService) {
				this.customerService = customerService;
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String name, email;
		}

		@Service
		@Transactional
		public static class CustomerService {

				private final JdbcTemplate jdbcTemplate;

				public CustomerService(JdbcTemplate jdbcTemplate) {
						this.jdbcTemplate = jdbcTemplate;
				}

				public Customer insert(String name, String email) {
						GeneratedKeyHolder generatedKeyHolder = new GeneratedKeyHolder();
						PreparedStatementCreator psc = connection -> {
								PreparedStatement ps = connection.prepareStatement("insert into customers(name, email) values (?, ?)", Statement.RETURN_GENERATED_KEYS);
								ps.setString(1, name);
								ps.setString(2, email);
								return ps;
						};
						this.jdbcTemplate.update(psc, generatedKeyHolder);
						Long id = generatedKeyHolder.getKey().longValue();
						return getById(id);
				}

				public Customer getById(Long id) {
						return this.jdbcTemplate.queryForObject("select * from customers where id =  ? ", rowMapper, id);
				}

				public Collection<Customer> all() {
						return this.jdbcTemplate.query("select * from customers", this.rowMapper);
				}

				private final RowMapper<Customer> rowMapper =
					(resultSet, i) -> new Customer(resultSet.getLong("id"), resultSet.getString("name"), resultSet.getString("email"));
		}

		private final CustomerService customerService;

		@Override
		public void run(ApplicationArguments args) {
				StringUtils.line();
				Stream.of("mitch,mia,valerie,jennifer".split(","))
					.forEach(name -> customerService.insert(name, name + "@" + name + ".com"));
				this.customerService.all().forEach(c -> log.info(customerService.getById(c.getId())));
		}
}

@Order(5)
@Log4j2
@Configuration
class SimpleJdbcUpdateCustomerServiceWriter implements ApplicationRunner {

		SimpleJdbcUpdateCustomerServiceWriter(CustomerService customerService) {
				this.customerService = customerService;
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String name, email;
		}

		@Service
		@Transactional
		public static class CustomerService {

				private final SimpleJdbcInsert customerSimpleJdbcInsert;
				private final CustomerMappingSqlQuery aCustomerMappingSqlQuery, allCustomersMappingSqlQuery;

				private static class CustomerMappingSqlQuery extends org.springframework.jdbc.object.MappingSqlQuery<Customer> {

						private CustomerMappingSqlQuery(DataSource dataSource, String sql, SqlParameter... parameters) {
								setDataSource(dataSource);
								setSql(sql);
								for (SqlParameter sp : parameters) {
										declareParameter(sp);
								}
								compile();
						}

						@Override
						protected Customer mapRow(ResultSet resultSet, int i) throws SQLException {
								return new Customer(resultSet.getLong("id"), resultSet.getString("name"), resultSet.getString("email"));
						}
				}

				public CustomerService(DataSource dataSource) {

						this.allCustomersMappingSqlQuery = new CustomerMappingSqlQuery(dataSource, "select * from customers");
						this.aCustomerMappingSqlQuery = new CustomerMappingSqlQuery(dataSource, "select * from customers where id = ? ", new SqlParameter("id", Types.INTEGER));

						this.customerSimpleJdbcInsert = new SimpleJdbcInsert(dataSource)
							.usingGeneratedKeyColumns("id")
							.withTableName("customers");
				}

				public Customer insert(String name, String email) {
						Map<String, Object> keys = new ConcurrentHashMap<>();
						keys.put("name", name);
						keys.put("email", email);
						Long newId = this.customerSimpleJdbcInsert.executeAndReturnKey(keys).longValue();
						return getById(newId);
				}

				public Customer getById(Long id) {
						return this.aCustomerMappingSqlQuery.findObject(id);
				}

				public Collection<Customer> all() {
						return this.allCustomersMappingSqlQuery.execute();
				}
		}

		private final CustomerService customerService;

		@Override
		public void run(ApplicationArguments args) {
				StringUtils.line();
				Stream.of("mitch,mia,valerie,jennifer".split(","))
					.forEach(name -> customerService.insert(name, name + "@" + name + ".com"));
				this.customerService.all().forEach(c -> log.info(customerService.getById(c.getId())));
		}
}

@Log4j2
abstract class StringUtils {

		public static void line() {
				log.info("============================================");
		}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Customer {

		@Id
		private Long id;
		private String name, email;
}

@Repository
interface CustomerRepository extends CrudRepository<Customer, Long> {
}

@Log4j2
@Configuration
@EnableJdbcRepositories
class SpringDataJdbcConfiguration {

		@EventListener(BeforeSaveEvent.class)
		public void beforeSave(BeforeSaveEvent evt) {
				Customer c = Customer.class.cast(evt.getEntity());
				log.info("about to saved the customer '" + c.getEmail() + "'");
		}

		@Bean
		NamingStrategy namingStrategy() {
				return new NamingStrategy() {
						@Override
						public String getSchema() {
								return NamingStrategy.super.getSchema();
						}

						@Override
						public String getTableName(Class<?> type) {
								return type.getSimpleName().toLowerCase() + "s";
						}

						@Override
						public String getColumnName(JdbcPersistentProperty property) {
								return NamingStrategy.super.getColumnName(property);
						}

						@Override
						public String getQualifiedTableName(Class<?> type) {
								return NamingStrategy.super.getQualifiedTableName(type);
						}

						@Override
						public String getReverseColumnName(JdbcPersistentProperty property) {
								return NamingStrategy.super.getReverseColumnName(property);
						}

						@Override
						public String getKeyColumn(JdbcPersistentProperty property) {
								return NamingStrategy.super.getKeyColumn(property);
						}

				};
		}

		@Component
		@Order(6)
		@Log4j2
		public static class SpringDataJdbc implements ApplicationRunner {

				private final CustomerRepository customerRepository;

				public SpringDataJdbc(CustomerRepository customerRepository) {
						this.customerRepository = customerRepository;
				}

				@Override
				public void run(ApplicationArguments args) throws Exception {
						StringUtils.line();
						customerRepository.save(new Customer(null, "violetta", "violetta@violetta.com"));
						customerRepository.findAll().forEach(log::info);
				}
		}

}

