package com.example.canatest;

import com.huazhu.basejarservice.canal.config.CanalClientConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CanaTestApplicationTests {

	@Autowired
	private CanalClientConfiguration configuration;

	@Test
	public void contextLoads() {
	}

}
