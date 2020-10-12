from matplotlib import pyplot as plt
import matplotlib.ticker as plticker
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler
import pandas as pd


def rebin(pd_in, col, thresh, side='underflow'):
    '''
    Function that fills underflow/overflow bins for visualization
    
    args:
        * pd_in : pandas data frame : type = pandas dataframe
        * col : column name to be altered : type = string
        * thresh : threshold : type = int or float
        * side : which bin (underflow or overflow) to put values : type = string
    returns:
        * : type = pandas series
    '''
    
    #correct a bug in python where chained indexing warning gets confused so turn it off
    pd.options.mode.chained_assignment = None  # default='warn'

    if side is 'underflow':
        pd_in.loc[pd_in[col] <= thresh, col] = thresh
    elif side is 'overflow':
        pd_in.loc[pd_in[col] >= thresh, col] = thresh
    else:
        print('need to specify underflow or overflow')
    return pd_in[col]

def single_hist(df_in,col,title=None,nbins=30,norm=False,xlabel=None,ylabel=None,xrng=(0,0)):
    
    if xrng != (0,0):
        df_in[col] = rebin(df_in,col,xrng[0],'underflow')
        df_in[col] = rebin(df_in,col,xrng[1],'overflow')
    
    
    plt.rc('font', family='serif')
    f,ax = plt.subplots(figsize=(10,5))
    ax.grid()
    hist_label = 'Entries {}\n$\mu$={:.4f}'.format(df_in.shape[0],df_in[col].mean())
    sns.distplot(df_in[col],bins=nbins,norm_hist=norm,kde=False,color='green',label=hist_label)#,hist_kws={'weights':plot_df['mrr']})
    plt.title(title)
    plt.legend(loc='best',fontsize=10)
    if xlabel:
        ax.set_xlabel(xlabel)
        
def plot_2dists(df1,df2,var_in,label1, label2,n_bins=30,xrng=(0,0)):
    '''
    inputs: df1 type=pd
            df2 type=p2
            var_in type=string desc: column in both df1,df2 to be plotted
            label1 type=string desc: label for df1
            label2 type=string desc: label for df2
            n_bins type=int desc: number of bins in output hist
    outputs: none, produces normalized histogram of var_in of df1 and df2 on same axis

    '''
    plt.rc('font', family='serif')
    f,ax = plt.subplots(figsize=(10,5))
    #ax.grid()

    if xrng[0]==0. and xrng[1]==0.:
        #set axis limits
        xmin = 0
        xmax = 0
        if df1[var_in].min() < df2[var_in].min():
            xmin = df1[var_in].min()
        else:
            xmin = df2[var_in].min()

        if df1[var_in].max() < df2[var_in].max():
            xmax = df2[var_in].max()
        else:
            xmax = df1[var_in].max()
        xrnge = (xmin, xmax*1.01)
    else:
        xrnge = xrng

    #plotting
    nbins=n_bins
    hist=True
    plt_kde=False
    hist_label1 = '{}, Entries {}'.format(label1,df1.shape[0])
    hist_label2 = '{}, Entries {}'.format(label2,df2.shape[0])
    line_color1 = 'blue'
    line_color2 = 'red'
    
    df1[var_in] = rebin(df1,var_in,xrnge[0],'underflow')
    df1[var_in] = rebin(df1,var_in,xrnge[1],'overflow')
    df2[var_in] = rebin(df2,var_in,xrnge[0],'underflow')
    df2[var_in] = rebin(df2,var_in,xrnge[1],'overflow')    
    
    sns.distplot(df1[var_in],bins=nbins,norm_hist=hist,kde=plt_kde,color=line_color1,label=hist_label1,hist_kws={"range":(xrnge[0],xrnge[1])})
    sns.distplot(df2[var_in],bins=nbins,norm_hist=hist,kde=plt_kde,color=line_color2,label=hist_label2,hist_kws={"range":(xrnge[0],xrnge[1])})

    plt.grid()
    plt.legend(loc='best',fontsize=10)
    ax.set_xlim(xrnge[0],xrnge[1])
    ax.set(xlabel=var_in, ylabel='normalized counts')
    plt.show()


def plot_2dists_v2(df1,df2,label1, label2,n_bins=30,xrng=(0,0)):
    '''
    inputs: df1 type=pd
            df2 type=pd
            label1 type=string desc: label for df1
            label2 type=string desc: label for df2
            n_bins type=int desc: number of bins in output hist
    outputs: none, produces normalized histogram of df1 and df2 on same axis

    '''
    plt.rc('font', family='serif')
    f,ax = plt.subplots(figsize=(10,5))
    ax.grid()
    #ax.set_xlim(None,20)

    if xrng[0]==0. and xrng[1]==0.:
        #set axis limits
        xmin = 0
        xmax = 0
        if df1[label1].min() < df2[label2].min():
            xmin = df1[label1].min()
        else:
            xmin = df2[label2].min()

        if df1[label1].max() < df2[label2].max():
            xmax = df2[label2].max()
        else:
            xmax = df1[label1].max()
        xrnge = (xmin, xmax*1.01)
    else:
        xrnge = xrng

    #plotting
    nbins=n_bins
    hist=True
    plt_kde=True
    hist_label1 = '{}, Entries {}'.format(label1,df1.shape[0])
    hist_label2 = '{}, Entries {}'.format(label2,df2.shape[0])
    line_color1 = 'blue'
    line_color2 = 'red'
    sns.distplot(df1[label1],bins=nbins,norm_hist=hist,kde=plt_kde,color=line_color1,label=hist_label1,hist_kws={"range":(xrnge[0],xrnge[1])})
    sns.distplot(df2[label2],bins=nbins,norm_hist=hist,kde=plt_kde,color=line_color2,label=hist_label2,hist_kws={"range":(xrnge[0],xrnge[1])})

    plt.legend(loc='best',fontsize=20)
    ax.set_xlim(xrnge[0],xrnge[1])
    ax.set(xlabel=label1, ylabel='normalized counts')
    ax.grid()
    plt.show()

    

def correlation_plot(X_in,features,title_txt = ''):
    scaler = MinMaxScaler()
    X_normd = scaler.fit_transform(X_in)
    
    features_corr = pd.DataFrame(X_normd,columns=features).corr()
    features_corr = features_corr.round(2)

    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(features_corr, 
                xticklabels=features_corr.columns,
                yticklabels=features_corr.columns, 
                annot=True,
                ax=ax)
    ax.set_title('Features Correlation {}'.format(title_txt), size=15)
    fig.show()

def plt_timeseries(var,timevar,df,title):
    #f,ax = plt.subplots(2,1,sharex=True,figsize=(15,7))
    f,ax = plt.subplots(figsize=(15,7))

    ax.scatter(df[timevar],df[var],color='green')
    # format the ticks
    f.autofmt_xdate()

    #Spacing between each line
    intervals = float(1)

    loc = plticker.MultipleLocator(base=intervals)
    ax.xaxis.set_major_locator(loc)
    #ax[0].yaxis.set_major_locator(loc)

    # Add the grid
    plt.grid()
    plt.xlabel(timevar)
    ax.set_ylabel(var)
    ax.grid(which='major', axis='both', linestyle='-')
    plt.xticks(fontsize=10)
    plt.title(title)
    plt.show()


################
##
# Plots categorical values directly from pandas
# inputs: str_list: list of pandas columns to plot
#         df_in: pandas dataframe
# output: None
##
################
def plot_strings(str_list,df_in):

    for plot in str_list:
        try:
            df_in[plot].hist(xrot=90)
            plt.ylabel('counts')
            plt.title(plot)
            plt.show()
        except Exception as e:
            print('ERROR plotting {}, exception={}'.format(plot,e))


def dedupe(df_in):
    '''
    De-duplicate Pandas datafarme and print info
    input: pandas dataframe
    returns: de-duped pandas dataframe  
    '''
    print('shape before de-dupe:{}'.format(df_in.shape))
    df_in_dedupe = df_in.drop_duplicates()
    print('shape after de-dupe:{}'.format(df_in_dedupe.shape))
    print('{} duplicate rows found'.format(df_in.shape[0]-df_in_dedupe.shape[0]))
    return df_in_dedupe


def check_quality(df_in_):
    '''
    Data Quality checker for Pandas, prints duplicates, null content of cols
    inputs: df_in_: pandas dataframe
    returns: None
    '''
    df_in = dedupe(df_in_)
    colz = df_in.columns

    nulls_counts = []
    for col in colz:
        pct_null = df_in[col].isna().sum()*100./df_in.shape[0]
        if pct_null > 0:
            nulls_counts.append(('{} is {:.1f}% NULL'.format(col, pct_null),pct_null))

    sorted_counts = sorted(nulls_counts,key=lambda x:x[1],reverse=True)
    print('\n---- \n')
    print('columns with null rows:')
    for j in sorted_counts:
        print(j[0])
    
