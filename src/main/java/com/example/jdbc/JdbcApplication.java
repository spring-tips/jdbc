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
import org.springframework.data.jdbc.mapping.model.NamingStrategy;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

@SpringBootApplication
public class JdbcApplication {

		@Bean
		PlatformTransactionManager dataSourceTransactionManager(DataSource dataSource) {
				return new DataSourceTransactionManager(dataSource);
		}

		@Bean
		TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
				return new TransactionTemplate(transactionManager);
		}

		@Bean
		JdbcTemplate jdbcTemplate(DataSource dataSource) {
				return new JdbcTemplate(dataSource);
		}

		public static void main(String[] args) {
				SpringApplication.run(JdbcApplication.class, args);
		}
}

@Log4j2
abstract class StringUtils {

		public static void line() {
				log.info("====================================");
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

				List<CustomerOrderReport> customerOrderReports = this.jdbcTemplate
					.query(" select c.*, (select count(o.id) from orders o where o.customer_fk = c.id ) as count from customers c ",
						(rs, rowNum) -> new CustomerOrderReport(rs.getLong("id"), rs.getString("email"), rs.getString("name"), rs.getInt("count")));
				customerOrderReports.forEach(log::info);

		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class CustomerOrderReport {
				private Long customerId;
				private String email, name;
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
						Customer currentCustomer = null;
						Function<ResultSet, Customer> customerMapper = input -> {
								try {
										return new Customer(input.getLong("cid"), input.getString("email"), input.getString("name"), new HashSet<>());
								}
								catch (Exception e) {
										throw new RuntimeException(e);
								}
						};

						Function<ResultSet, Order> orderMapper = input -> {
								try {
										return new Order(rs.getLong("oid"), rs.getString("sku"));
								}
								catch (SQLException e) {
										throw new RuntimeException(e);
								}
						};

						while (rs.next()) {
								long id = rs.getLong("cid");

								if (currentCustomer == null || currentCustomer.getId() != id) {
										currentCustomer = customerMapper.apply(rs);
								}

								currentCustomer.getOrders().add(orderMapper.apply(rs));
								customerMap.put(currentCustomer.getId(), currentCustomer);
						}

						return customerMap.values();
				};

				Collection<Customer> customers = this.jdbcTemplate.query("select c.id as cid, c.*, o.id as oid, o.* from customers c left join orders o on c.id = o.customer_fk order by cid", customerResultSetExtractor);
				customers.forEach(log::info);
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String email, name;
				private Set<Order> orders = new HashSet<>();
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Order {
				private Long id;
				private String sku;
		}

}


@Log4j2
@Order(3)
@Component
class QueryCustomersAndOrdersSimpleFlatMapper implements ApplicationRunner {

		private final JdbcTemplate jdbcTemplate;

		QueryCustomersAndOrdersSimpleFlatMapper(JdbcTemplate jdbcTemplate) {
				this.jdbcTemplate = jdbcTemplate;
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {

				StringUtils.line();

				ResultSetExtractorImpl<Customer> customerResultSetExtractor =
					JdbcTemplateMapperFactory
						.newInstance()
						.addKeys("id")
						.newResultSetExtractor(Customer.class);

				Collection<Customer> customers = this.jdbcTemplate.query("select c.id as  id, c.name as name, c.email as email, o.id as orders_id , o.sku as orders_sku from customers c left join orders o on c.id = o.customer_fk", customerResultSetExtractor);
				customers.forEach(log::info);
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String email, name;
				private Set<Order> orders = new HashSet<>();
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Order {
				private Long id;
				private String sku;
		}
}

@Log4j2
@Order(4)
@Configuration
class JdbcTemplateCustomerServiceWriter implements ApplicationRunner {


		JdbcTemplateCustomerServiceWriter(TransactionTemplate transactionTemplate, JdbcTemplateCustomerService customerService) {
				this.transactionTemplate = transactionTemplate;
				this.customerService = customerService;
		}

		@Service
		@Transactional
		public static class JdbcTemplateCustomerService {

				private final JdbcTemplate jdbcTemplate;

				public JdbcTemplateCustomerService(JdbcTemplate jdbcTemplate) {
						this.jdbcTemplate = jdbcTemplate;
				}

				public Customer insert(String name, String email) {
						GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
						PreparedStatementCreator psc = con -> {
								PreparedStatement ps = con.prepareStatement("insert into customers(name,email) values( ?, ?)", Statement.RETURN_GENERATED_KEYS);
								ps.setString(1, name);
								ps.setString(2, email);
								return ps;
						};
						this.jdbcTemplate.update(psc, keyHolder);
						Long id = keyHolder.getKey().longValue();
						return byId(id);
				}

				public Customer byId(Long id) {
						return jdbcTemplate.queryForObject("select * from customers c where c.id = ? ",
							(rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("email"), rs.getString("name")), id);
				}

				public Collection<Customer> all() {
						return jdbcTemplate.query("select * from customers c",
							(rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("email"), rs.getString("name")));
				}
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {
				StringUtils.line();
				try {

						transactionTemplate.execute(new TransactionCallbackWithoutResult() {
								@Override
								protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
										Stream.of("A", "B", "C").forEach(name -> customerService.insert(name, name + "@" + name + ".com"));
//										throw new RuntimeException("monkey wrench!");
								}
						});
				}
				catch (RuntimeException e) {
						log.error(e);
				}
				customerService.all().forEach(c -> log.info(customerService.byId(c.getId())));
		}

		private final TransactionTemplate transactionTemplate;
		private final JdbcTemplateCustomerService customerService;

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String email, name;
		}
}

@Order(5)
@Log4j2
@Configuration
class SimpleJdbcCustomerServiceWriter implements ApplicationRunner {

		private final SimpleCustomerService customerService;

		SimpleJdbcCustomerServiceWriter(SimpleCustomerService customerService) {
				this.customerService = customerService;
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class Customer {
				private Long id;
				private String email, name;
		}

		@Service
		@Transactional
		public static class SimpleCustomerService {

				public SimpleCustomerService(DataSource ds) {
						this.all = new CustomerMappingSqlQuery(ds, "select * from customers");
						this.byId = new CustomerMappingSqlQuery(ds, "select * from customers where id = ? ", new SqlParameter("id", Types.INTEGER));

						this.insert = new SimpleJdbcInsert(ds)
							.usingGeneratedKeyColumns("id")
							.withTableName("customers");
				}

				private static class CustomerMappingSqlQuery extends org.springframework.jdbc.object.MappingSqlQuery<Customer> {


						public CustomerMappingSqlQuery(DataSource ds, String sql, SqlParameter... parameters) {
								setDataSource(ds);
								setSql(sql);

								for (SqlParameter p : parameters)
										declareParameter(p);

								compile();
						}

						@Override
						protected Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
								return new Customer(rs.getLong("id"), rs.getString("name"), rs.getString("email"));
						}

				}

				private final CustomerMappingSqlQuery all, byId;
				private final SimpleJdbcInsert insert;

				public Customer insert(String name, String email) {
						Map<String, Object> params = new HashMap<>();
						params.put("name", name);
						params.put("email", email);
						Long newId = this.insert.executeAndReturnKey(params).longValue();
						return byId(newId);
				}

				public Collection<Customer> all() {
						return this.all.execute();
				}

				public Customer byId(Long id) {
						return this.byId.findObject(id);
				}
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {
				StringUtils.line();
				Stream.of("A", "B", "C").forEach(name -> customerService.insert(name, name + "@" + name + ".com"));
				customerService.all().forEach(c -> log.info(customerService.byId(c.getId())));
		}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Customer {

		@Id
		private Long id;
		private String email, name;
}

@Repository
interface CustomerRepository extends CrudRepository<Customer, Long> {
}

@Configuration
@EnableJdbcRepositories
@Log4j2
class SpringDataJdbcConfiguration {

		@EventListener(BeforeSaveEvent.class)
		public void before(BeforeSaveEvent event) {
				Customer customer = Customer.class.cast(event.getEntity());
				log.info("saving .. " + customer);
		}

		@Bean
		NamingStrategy namingStrategy() {
				return new NamingStrategy() {
						@Override
						public String getTableName(Class<?> type) {
								return type.getSimpleName().toLowerCase() + "s";
						}
				};
		}

		@Component
		public static class SpringDataJdbc implements ApplicationRunner {

				private final CustomerRepository customerRepository;

				public SpringDataJdbc(CustomerRepository customerRepository) {
						this.customerRepository = customerRepository;
				}

				@Override
				public void run(ApplicationArguments args) throws Exception {
						StringUtils.line();
						Stream.of("D", "E", "F").forEach(name -> customerRepository.save(new Customer(null, name, name + "@" + name + ".com")));
						customerRepository.findAll().forEach(c -> log.info(customerRepository.findById(c.getId()).get()));
				}
		}
}
