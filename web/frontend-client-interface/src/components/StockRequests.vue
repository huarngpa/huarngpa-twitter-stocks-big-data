<template>
  <div>
    <section class="hero is-primary">
      <div class="hero-body">
        <div class="container has-text-centered">
          <h2 class="title">Stock Ticker Requests</h2>
          <p class="subtitle error-msg">{{ errorMsg }}</p>
        </div>
      </div>
    </section>
    <section class="section">
      <div class="container">
        <div class="columns">
          <div class="column">
            <div class="field">
              <label class="label is-large" for="twitterUser">Stock Ticker:</label>
              <div class="control">
                <input type="text" class="input is-large" id="stockTicker" v-model="stockTicker">
              </div>
              <br/>
              <div control="control">
                <a class="button is-large is-primary" @click="requesting">Request</a>
              </div>
            </div>
          </div>
          <div class="column">
            <table class="table is-fullwidth">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Stock Ticker</th>
                  <th>Requested At</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="stockTicker in stockRequests" v-bind:key="stockTicker.ticker">
                  <td>{{ stockTicker.id }}</td>
                  <td>{{ stockTicker.ticker }}</td>
                  <td>{{ stockTicker.requested_at }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
      <div class="container">
      </div>
    </section>
  </div>
</template>

<script>
import { mapState } from 'vuex'
import { EventBus } from '@/utils'

export default {
  data () {
    return {
      stockTicker: '',
      errorMsg: ''
    }
  },
  computed: mapState({
    stockRequests: state => state.stockRequests
  }),
  methods: {
    requesting () {
      this.$store.dispatch('requestNewStockTicker', { stockTicker: this.stockTicker })
    }
  },
  beforeMount () {
    this.$store.dispatch('loadStockRequests')
  },
  mounted () {
    EventBus.$on('stockRequestSucceeded', (msg) => {
      this.errorMsg = msg
    })
    EventBus.$on('stockRequestFailed', (msg) => {
      this.errorMsg = msg
    })
  },
  beforeDestroy () {
    EventBus.$off('stockRequestSucceeded')
    EventBus.$off('stockRequestFailed')
  }
}
</script>

<style scoped>
</style>
