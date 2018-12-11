<template>
  <div>
    <section class="hero is-primary">
      <div class="hero-body">
        <div class="container has-text-centered">
          <h2 class="title">Twitter User Requests</h2>
          <p class="subtitle error-msg">{{ errorMsg }}</p>
        </div>
      </div>
    </section>
    <section class="section">
      <div class="container">
        <div class="columns">
          <div class="column">
            <div class="field">
              <label class="label is-large" for="twitterUser">Twitter User:</label>
              <div class="control">
                <input type="text" class="input is-large" id="twitterUser" v-model="twitterUser">
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
                  <th>Twitter User</th>
                  <th>Requested At</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="twitterUser in twitterRequests" v-bind:key="twitterUser.user_id">
                  <td>{{ twitterUser.id }}</td>
                  <td>{{ twitterUser.username }}</td>
                  <td>{{ twitterUser.requested_at }}</td>
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
// import { EventBus } from '@/utils'

export default {
  data () {
    return {
      twitterUser: '',
      errorMsg: ''
    }
  },
  computed: mapState({
    twitterRequests: state => state.twitterRequests
  }),
  methods: {
    requesting () {
      this.$dispatch('requestNewTwitterUser', { twitterUser: this.twitterUser })
    }
  },
  beforeMount () {
    this.$store.dispatch('loadTwitterRequests')
  }
}
</script>

<style scoped>
</style>
