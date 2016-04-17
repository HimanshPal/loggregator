package config_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"trafficcontroller/config"
)

var _ = Describe("Config", func() {
	Describe("ParseConfig", func() {
		var (
			logLevel    = false
			logFilePath = "../test_assets/stdout.log"
		)

		It("reads the outgoing dropsonde port from config", func() {
			configFile := "../test_assets/minimal_loggregator_trafficcontroller.json"

			var c *config.Config

			c, _ = config.ParseConfig(logLevel, configFile, logFilePath)

			Expect(c.OutgoingDropsondePort).To(Equal(uint32(4566)))
		})

		Context("without ETCD/heartbeat specific configuration", func() {
			It("uses defaults", func() {
				configFile := "../test_assets/minimal_loggregator_trafficcontroller.json"

				var c *config.Config

				c, _ = config.ParseConfig(logLevel, configFile, logFilePath)

				Expect(c.JobName).To(Equal("loggregator_trafficcontroller"))
				Expect(c.JobIndex).To(Equal(0))
				Expect(c.EtcdMaxConcurrentRequests).To(Equal(10))
			})
		})

		Context("with ETCD/heartbeat specific configuration", func() {
			It("uses specified properties", func() {
				configFile := "../test_assets/loggregator_trafficcontroller.json"

				var c *config.Config

				c, _ = config.ParseConfig(logLevel, configFile, logFilePath)

				Expect(c.JobName).To(Equal("trafficcontroller"))
				Expect(c.JobIndex).To(Equal(3))
				Expect(c.EtcdMaxConcurrentRequests).To(Equal(5))
				Expect(c.EtcdUrls).To(ConsistOf([]string{"http://127.0.0.1:4001", "http://127.0.0.1:4002"}))
			})
		})

		Context("without MonitorIntervalSeconds", func() {
			It("defaults MonitorIntervalSeconds to 60 seconds", func() {
				configFile := "../test_assets/loggregator_trafficcontroller.json"

				var c *config.Config

				c, _ = config.ParseConfig(logLevel, configFile, logFilePath)
				Expect(c.MonitorIntervalSeconds).To(Equal(uint(60)))
			})
		})

		Context("without SecurityEventLog", func() {
			It("defaults SecurityEventLog to empty string", func() {
				configFile := "../test_assets/minimal_loggregator_trafficcontroller.json"

				c, err := config.ParseConfig(logLevel, configFile, logFilePath)
				Expect(err).ToNot(HaveOccurred())
				Expect(c.SecurityEventLog).To(Equal(""))
			})
		})

		Context("with SecurityEventLog", func() {
			It("uses specified properties", func() {
				configFile := "../test_assets/loggregator_trafficcontroller.json"

				c, err := config.ParseConfig(logLevel, configFile, logFilePath)
				Expect(err).To(Succeed())
				Expect(c.SecurityEventLog).To(Equal("access.log"))
			})
		})
	})
})
